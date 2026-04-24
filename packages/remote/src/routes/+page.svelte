<script>
	const SERVICE_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10001';
	const STATUS_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10002';
	const CONTROL_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10003';
	const PREVIEW_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10004';
	const PREVIEW_OFFSET_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10005';
	const STATUS_OFFSET_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10006';

	/** @typedef {{ connect: () => Promise<BluetoothRemoteGATTServer> }} BluetoothRemoteGATT */
	/** @typedef {{ getCharacteristic: (uuid: string) => Promise<BluetoothRemoteGATTCharacteristic> }} BluetoothRemoteGATTService */
	/** @typedef {{ readValue: () => Promise<DataView>, writeValue: (value: BufferSource) => Promise<void>, writeValueWithResponse?: (value: BufferSource) => Promise<void> }} BluetoothRemoteGATTCharacteristic */
	/** @typedef {{ getPrimaryService: (uuid: string) => Promise<BluetoothRemoteGATTService> }} BluetoothRemoteGATTServer */
	/** @typedef {{ name?: string, gatt: BluetoothRemoteGATT, addEventListener: (event: 'gattserverdisconnected', handler: () => void) => void }} BluetoothDevice */
	/** @typedef {{ requestDevice: (options: object) => Promise<BluetoothDevice> }} BluetoothApi */
	/** @typedef {{ name: string, target_mbps?: number | null, split_percentage?: number | null, tx_packets?: number, tx_bytes?: number, tx_mbps?: number }} InterfaceStatus */
	/** @typedef {{ mode: string, effective_strategy: string, packets: number, payload_bytes: number, interfaces: InterfaceStatus[] }} ControlStatus */
	/** @typedef {{ enabled: boolean, decoding: boolean, jpeg_bytes: number, characteristic: string, offset_characteristic: string, chunk_bytes: number }} PreviewStatus */
	/** @typedef {{ control: ControlStatus, preview?: PreviewStatus }} RemoteStatus */
	/** @typedef {{ mode?: string, targets_mbps?: Record<string, number>, split_percentages?: Record<string, number>, preview_enabled?: boolean }} ControlPatch */

	/** @type {BluetoothDevice | null} */
	let device = $state(null);
	/** @type {BluetoothRemoteGATTServer | null} */
	let server = $state(null);
	/** @type {RemoteStatus | null} */
	let status = $state(null);
	let error = $state('');
	let connecting = $state(false);
	let saving = $state(false);
	let polling = $state(true);
	let autoPreview = $state(false);
	let previewIntervalSeconds = $state(10);
	let readingStatus = false;
	let previewUrl = $state('');
	let loadingPreview = $state(false);
	let lastPreviewBytes = $state(0);
	/** @type {Promise<unknown>} */
	let gattQueue = Promise.resolve();

	let connected = $derived(Boolean(server));
	let interfaces = $derived(getInterfaces(status));
	let mode = $derived(getMode(status));
	let effectiveStrategy = $derived(getEffectiveStrategy(status));

	/** @param {RemoteStatus | null} value */
	function getInterfaces(value) {
		return value?.control.interfaces ?? [];
	}

	/** @param {RemoteStatus | null} value */
	function getMode(value) {
		return value?.control.mode ?? 'auto';
	}

	/** @param {RemoteStatus | null} value */
	function getEffectiveStrategy(value) {
		return value?.control.effective_strategy ?? 'redundant';
	}

	async function connect() {
		error = '';
		connecting = true;

		try {
			const bluetooth = /** @type {Navigator & { bluetooth?: BluetoothApi }} */ (navigator).bluetooth;
			if (!bluetooth) throw new Error('Web Bluetooth is not available in this browser.');

			const selectedDevice = await bluetooth.requestDevice({
				filters: [{ namePrefix: 'irohsion' }],
				optionalServices: [SERVICE_UUID]
			});
			device = selectedDevice;
			device.addEventListener('gattserverdisconnected', () => {
				server = null;
				status = null;
				clearPreview();
			});

			server = await selectedDevice.gatt.connect();
			await readStatus();
		} catch (err) {
			error = err instanceof Error ? err.message : String(err);
			server = null;
		} finally {
			connecting = false;
		}
	}

	async function readStatus() {
		if (!server || readingStatus) return;
		readingStatus = true;

		try {
			await enqueueGatt(readStatusNow);
		} finally {
			readingStatus = false;
		}
	}

	async function readStatusNow() {
		const activeServer = server;
		if (!activeServer) return;
		const service = await retryGatt(() => activeServer.getPrimaryService(SERVICE_UUID));
		const characteristic = await retryGatt(() => service.getCharacteristic(STATUS_UUID));
		const offsetCharacteristic = await retryGatt(() => service.getCharacteristic(STATUS_OFFSET_UUID));
		status = await readJsonStatus(characteristic, offsetCharacteristic);
	}

	async function readPreview() {
		if (!server || loadingPreview || !status?.preview?.decoding) return;
		loadingPreview = true;
		error = '';

		try {
			await enqueueGatt(readPreviewNow);
		} catch (err) {
			error = err instanceof Error ? err.message : String(err);
		} finally {
			loadingPreview = false;
		}
	}

	async function readPreviewNow() {
		const activeServer = server;
		if (!activeServer) return;
		const service = await retryGatt(() => activeServer.getPrimaryService(SERVICE_UUID));
		const previewCharacteristic = await retryGatt(() => service.getCharacteristic(PREVIEW_UUID));
		const offsetCharacteristic = await retryGatt(() => service.getCharacteristic(PREVIEW_OFFSET_UUID));
		let expectedBytes = status?.preview?.jpeg_bytes ?? 0;
		if (expectedBytes === 0) {
			await readStatusNow();
			expectedBytes = status?.preview?.jpeg_bytes ?? 0;
		}
		const bytes = await readChunkedCharacteristic(previewCharacteristic, offsetCharacteristic, expectedBytes);
		const blob = new Blob([bytes], { type: 'image/jpeg' });
		clearPreview();
		previewUrl = URL.createObjectURL(blob);
		lastPreviewBytes = bytes.byteLength;
		await readStatusNow();
	}

	function clearPreview() {
		if (previewUrl) URL.revokeObjectURL(previewUrl);
		previewUrl = '';
	}

	/**
	 * @template T
	 * @param {() => Promise<T>} operation
	 * @returns {Promise<T>}
	 */
	function enqueueGatt(operation) {
		const run = gattQueue.then(operation, operation);
		gattQueue = run.catch(() => {});
		return run;
	}

	/** @param {number} ms */
	function sleep(ms) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	/**
	 * @template T
	 * @param {() => Promise<T>} operation
	 * @returns {Promise<T>}
	 */
	async function retryGatt(operation) {
		let lastError = null;
		for (let attempt = 0; attempt < 3; attempt += 1) {
			try {
				return await operation();
			} catch (err) {
				lastError = err;
				await sleep(120 + attempt * 180);
			}
		}
		throw lastError;
	}

	/**
	 * @param {BluetoothRemoteGATTCharacteristic} characteristic
	 * @returns {Promise<DataView>}
	 */
	function readCharacteristic(characteristic) {
		return retryGatt(() => characteristic.readValue());
	}

	/**
	 * @param {BluetoothRemoteGATTCharacteristic} characteristic
	 * @param {BufferSource} value
	 */
	async function writeCharacteristic(characteristic, value) {
		await retryGatt(async () => {
			if (characteristic.writeValueWithResponse) {
				await characteristic.writeValueWithResponse(value);
				return;
			}
			await characteristic.writeValue(value);
		});
	}

	/**
	 * @param {BluetoothRemoteGATTCharacteristic} characteristic
	 * @param {BluetoothRemoteGATTCharacteristic} offsetCharacteristic
	 * @param {number | null} expectedBytes
	 */
	async function readChunkedCharacteristic(characteristic, offsetCharacteristic, expectedBytes = null) {
		const chunks = [];
		let totalBytes = 0;
		let offset = 0;
		let reads = 0;
		while ((expectedBytes === null || offset < expectedBytes) && reads < 128) {
			const offsetBytes = new Uint8Array(4);
			new DataView(offsetBytes.buffer).setUint32(0, offset, true);
			await writeCharacteristic(offsetCharacteristic, offsetBytes);
			await sleep(20);

			const value = await readCharacteristic(characteristic);
			const chunk = new Uint8Array(value.byteLength);
			chunk.set(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
			if (chunk.byteLength === 0) break;
			chunks.push(chunk);
			offset += chunk.byteLength;
			totalBytes += chunk.byteLength;
			reads += 1;
		}

		const bytes = new Uint8Array(totalBytes);
		let cursor = 0;
		for (const chunk of chunks) {
			bytes.set(chunk, cursor);
			cursor += chunk.byteLength;
		}
		return bytes;
	}

	/**
	 * @param {BluetoothRemoteGATTCharacteristic} characteristic
	 * @param {BluetoothRemoteGATTCharacteristic} offsetCharacteristic
	 */
	async function readJsonStatus(characteristic, offsetCharacteristic) {
		let lastText = '';
		for (let attempt = 0; attempt < 2; attempt += 1) {
			const bytes = await readChunkedCharacteristic(characteristic, offsetCharacteristic);
			lastText = new TextDecoder().decode(bytes);
			try {
				return JSON.parse(lastText);
			} catch (err) {
				if (attempt === 1) {
					const suffix = lastText.slice(Math.max(0, lastText.length - 120));
					throw new Error(`${err instanceof Error ? err.message : String(err)}; status bytes=${bytes.byteLength}; tail=${suffix}`);
				}
				await sleep(200);
			}
		}
		throw new Error('status JSON read failed');
	}

	/** @param {ControlPatch} patch */
	async function writePatch(patch) {
		if (!server) return;
		saving = true;
		error = '';

		try {
			await enqueueGatt(async () => {
				const activeServer = server;
				if (!activeServer) return;
				const service = await retryGatt(() => activeServer.getPrimaryService(SERVICE_UUID));
				const characteristic = await retryGatt(() => service.getCharacteristic(CONTROL_UUID));
				await writeCharacteristic(characteristic, new TextEncoder().encode(JSON.stringify(patch)));
				await readStatusNow();
			});
		} catch (err) {
			error = err instanceof Error ? err.message : String(err);
		} finally {
			saving = false;
		}
	}

	/** @param {string} nextMode */
	function setMode(nextMode) {
		void writePatch({ mode: nextMode });
	}

	/** @param {string} interfaceName @param {string | number} value */
	function setTarget(interfaceName, value) {
		void writePatch({ targets_mbps: { [interfaceName]: Number(value) } });
	}

	/** @param {string} interfaceName @param {string | number} value */
	function setPercentage(interfaceName, value) {
		void writePatch({ split_percentages: { [interfaceName]: Number(value) } });
	}

	/** @param {boolean} enabled */
	function setPreviewEnabled(enabled) {
		if (!enabled) {
			autoPreview = false;
			clearPreview();
			lastPreviewBytes = 0;
		}
		void writePatch({ preview_enabled: enabled });
	}

	/** @param {InterfaceStatus} iface */
	function displayPercentage(iface) {
		return iface.split_percentage ?? evenPercentage();
	}

	function evenPercentage() {
		return interfaces.length > 0 ? Math.round(100 / interfaces.length) : 0;
	}

	/** @param {number | null | undefined} value */
	function formatMbps(value) {
		return `${(value ?? 0).toFixed(2)} Mbps`;
	}

	/** @param {number | null | undefined} value */
	function formatBytes(value) {
		const bytes = value ?? 0;
		if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(2)} GB`;
		if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(2)} MB`;
		if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(1)} KB`;
		return `${bytes} B`;
	}

	function totalInterfaceMbps() {
		return interfaces.reduce((total, iface) => total + (iface.tx_mbps ?? 0), 0);
	}

	function totalInterfaceBytes() {
		return interfaces.reduce((total, iface) => total + (iface.tx_bytes ?? 0), 0);
	}

	/** @param {Element} _node */
	function pollRemote(_node) {
		let lastPreviewAt = 0;
		const statusInterval = setInterval(() => {
			if (polling && !loadingPreview && !saving) void readStatus();
		}, 1000);

		const previewInterval = setInterval(() => {
			if (!polling || !autoPreview || !status?.preview?.decoding) return;
			const now = Date.now();
			if (now - lastPreviewAt < Math.max(1, previewIntervalSeconds) * 1000) return;
			const bytes = status?.preview?.jpeg_bytes ?? 0;
			if (bytes > 0 && bytes !== lastPreviewBytes && !loadingPreview) {
				lastPreviewAt = now;
				void readPreview();
			}
		}, 1000);

		return () => {
			clearInterval(statusInterval);
			clearInterval(previewInterval);
		};
	}
</script>

<svelte:head>
	<title>Irohsion Remote</title>
</svelte:head>

<main class="min-h-svh bg-black px-3 pt-[max(0.5rem,env(safe-area-inset-top))] pb-[max(0.5rem,env(safe-area-inset-bottom))] text-neutral-100">
	<div class="mx-auto grid max-w-xl gap-3">
		<header class="flex items-center justify-between gap-3">
			<div class="flex items-center gap-2">
				<button
					class="grid h-10 w-10 place-items-center rounded-full border border-white/10 bg-neutral-950 active:scale-[0.98]"
					onclick={connect}
					disabled={connecting}
					aria-label={connected ? 'Connected' : 'Connect'}
				>
					<span
						class={[
							'h-3.5 w-3.5 rounded-full',
							connected ? 'bg-emerald-400 shadow-[0_0_18px_rgba(52,211,153,0.9)]' : 'bg-red-500'
						]}
					></span>
				</button>
				{#if status}
					<button
						class={[
							'rounded-full px-3 py-2 text-xs font-black',
							polling ? 'bg-emerald-400 text-black' : 'bg-neutral-950 text-neutral-400'
						]}
						onclick={() => (polling = !polling)}
					>
						{polling ? 'Poll' : 'Off'}
					</button>
				{/if}
			</div>

			<div class="flex min-w-0 flex-1 items-center justify-end gap-2 text-right">
				{#if status}
					<div class="rounded-2xl bg-neutral-950 px-3 py-2">
						<p class="text-[0.6rem] font-bold text-neutral-600 uppercase">Mbps</p>
						<p class="text-sm font-black text-emerald-400">{totalInterfaceMbps().toFixed(2)}</p>
					</div>
					<div class="rounded-2xl bg-neutral-950 px-3 py-2">
						<p class="text-[0.6rem] font-bold text-neutral-600 uppercase">TX</p>
						<p class="text-sm font-black">{formatBytes(totalInterfaceBytes())}</p>
					</div>
					<div class="rounded-2xl bg-neutral-950 px-3 py-2">
						<p class="text-[0.6rem] font-bold text-neutral-600 uppercase">Pkts</p>
						<p class="text-sm font-black">{status.control?.packets ?? 0}</p>
					</div>
				{:else}
					<p class="truncate text-sm font-bold text-neutral-400">{device?.name ?? 'No device'}</p>
				{/if}
			</div>
		</header>

		{#if error}
			<section class="rounded-3xl border border-red-500/30 bg-red-950/30 p-4 text-sm text-red-100">
				{error}
			</section>
		{/if}

		{#if status}
			<section {@attach pollRemote} class="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-2 rounded-3xl border border-white/10 bg-neutral-950 p-3">
				<p class="min-w-0 truncate text-xl font-black tracking-[-0.04em]">{effectiveStrategy}</p>
				<div class="grid grid-cols-3 gap-1.5">
					{#each ['auto', 'split', 'redundant'] as option}
						<button
							class={[
								'rounded-2xl px-2.5 py-2 text-xs font-black capitalize',
								mode === option ? 'bg-emerald-400 text-black' : 'bg-neutral-900 text-neutral-300'
							]}
							onclick={() => setMode(option)}
							disabled={saving}
						>
							{option}
						</button>
					{/each}
				</div>
			</section>

			<section class="grid gap-2">
				{#each interfaces as iface}
					<article class="rounded-3xl border border-white/10 bg-neutral-950 p-3">
						<div class="mb-3 grid grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
							<h2 class="min-w-0 truncate text-xl font-black tracking-[-0.05em]">{iface.name}</h2>
							<div class="grid grid-cols-3 gap-1.5">
								<div class="min-w-16 rounded-2xl bg-black px-2 py-1.5 text-right">
									<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">Rate</p>
									<p class="text-xs font-black text-emerald-400">{formatMbps(iface.tx_mbps)}</p>
								</div>
								<div class="min-w-16 rounded-2xl bg-black px-2 py-1.5 text-right">
									<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">TX</p>
									<p class="text-xs font-black text-neutral-100">{formatBytes(iface.tx_bytes)}</p>
								</div>
								<div class="min-w-16 rounded-2xl bg-black px-2 py-1.5 text-right">
									<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">Pkts</p>
									<p class="text-xs font-black text-neutral-100">{iface.tx_packets ?? 0}</p>
								</div>
							</div>
						</div>

						<label class="grid gap-1.5">
							<div class="flex justify-between text-xs font-bold text-neutral-400">
								<span>Target Mbps</span>
								<span>{iface.target_mbps ?? 0}</span>
							</div>
							<input
								class="accent-emerald-400"
								type="range"
								min="0"
								max="50"
								step="0.5"
								value={iface.target_mbps ?? 0}
								onchange={(event) => setTarget(iface.name, event.currentTarget.value)}
							/>
						</label>

						{#if mode === 'split'}
							<label class="mt-3 grid gap-1.5">
								<div class="flex justify-between text-xs font-bold text-neutral-400">
									<span>Split percentage</span>
									<span>{displayPercentage(iface)}%</span>
								</div>
								<input
									class="accent-sky-400"
									type="range"
									min="0"
									max="100"
									step="1"
									value={displayPercentage(iface)}
									onchange={(event) => setPercentage(iface.name, event.currentTarget.value)}
								/>
							</label>
						{/if}
					</article>
				{/each}
			</section>

			<section class="rounded-3xl border border-white/10 bg-neutral-950 p-4">
				<div class="flex items-center justify-between gap-4">
					<div>
						<p class="text-xs font-black tracking-[0.18em] text-neutral-500 uppercase">Preview</p>
						<p class="text-sm text-neutral-400">{status.preview?.jpeg_bytes ?? 0} bytes ready</p>
					</div>
					<button
						class="rounded-full bg-white px-4 py-2 text-sm font-black text-black disabled:opacity-50"
						onclick={readPreview}
						disabled={loadingPreview || !status.preview?.decoding || !status.preview?.jpeg_bytes}
					>
						{loadingPreview ? 'Loading...' : 'Load'}
					</button>
				</div>

				<div class="mt-4 grid gap-3 rounded-2xl bg-black p-3">
					<label class="flex items-center justify-between gap-4 text-sm font-bold text-neutral-300">
						<span>Decoder</span>
						<input
							class="h-5 w-5 accent-emerald-400"
							type="checkbox"
							checked={status.preview?.decoding ?? false}
							onchange={(event) => setPreviewEnabled(event.currentTarget.checked)}
						/>
					</label>
					<label class="flex items-center justify-between gap-4 text-sm font-bold text-neutral-300">
						<span>Auto preview</span>
						<input class="h-5 w-5 accent-emerald-400" type="checkbox" bind:checked={autoPreview} disabled={!status.preview?.decoding} />
					</label>
					<label class="grid gap-2">
						<div class="flex justify-between text-xs font-bold text-neutral-500">
							<span>Decode interval</span>
							<span>{previewIntervalSeconds}s</span>
						</div>
						<input
							class="accent-emerald-400"
							type="range"
							min="5"
							max="60"
							step="5"
							bind:value={previewIntervalSeconds}
						/>
					</label>
				</div>

				{#if previewUrl}
					<img class="mt-4 w-full rounded-2xl border border-white/10 bg-black" src={previewUrl} alt="Latest decoded preview frame" />
				{:else}
					<div class="mt-4 grid aspect-video place-items-center rounded-2xl border border-dashed border-white/10 text-sm text-neutral-600">
						No frame loaded
					</div>
				{/if}
			</section>
		{:else}
			<section class="rounded-3xl border border-white/10 bg-neutral-950 p-5">
				<p class="text-neutral-400">Connect to the irohsion BLE remote to tune paths.</p>
			</section>
		{/if}
	</div>
</main>

<script>
	const SERVICE_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10001';
	const STATUS_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10002';
	const STATUS_OFFSET_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10006';

	/** @typedef {{ connect: () => Promise<BluetoothRemoteGATTServer> }} BluetoothRemoteGATT */
	/** @typedef {{ getCharacteristic: (uuid: string) => Promise<BluetoothRemoteGATTCharacteristic> }} BluetoothRemoteGATTService */
	/** @typedef {{ readValue: () => Promise<DataView>, writeValue: (value: BufferSource) => Promise<void>, writeValueWithResponse?: (value: BufferSource) => Promise<void> }} BluetoothRemoteGATTCharacteristic */
	/** @typedef {{ getPrimaryService: (uuid: string) => Promise<BluetoothRemoteGATTService> }} BluetoothRemoteGATTServer */
	/** @typedef {{ name?: string, gatt: BluetoothRemoteGATT, addEventListener: (event: 'gattserverdisconnected', handler: () => void) => void }} BluetoothDevice */
	/** @typedef {{ requestDevice: (options: object) => Promise<BluetoothDevice> }} BluetoothApi */
	/** @typedef {{ name: string, status?: 'connected' | 'reconnecting' | 'dead', tx_packets?: number, tx_bytes?: number, tx_mbps?: number, server_mbps?: number | null, server_last_seq?: number | null, server_max_seq?: number | null }} InterfaceStatus */
	/** @typedef {{ mode: string, effective_strategy: string, packets: number, payload_bytes: number, interfaces: InterfaceStatus[] }} ControlStatus */
	/** @typedef {{ control: ControlStatus }} RemoteStatus */

	/** @type {BluetoothDevice | null} */
	let device = $state(null);
	/** @type {BluetoothRemoteGATTServer | null} */
	let server = $state(null);
	/** @type {RemoteStatus | null} */
	let status = $state(null);
	let error = $state('');
	let connecting = $state(false);
	let polling = $state(true);
	let readingStatus = false;
	/** @type {Promise<unknown>} */
	let gattQueue = Promise.resolve();

	let connected = $derived(Boolean(server));
	let interfaces = $derived(getInterfaces(status));
	let effectiveStrategy = $derived(getEffectiveStrategy(status));

	/** @param {RemoteStatus | null} value */
	function getInterfaces(value) {
		return value?.control.interfaces ?? [];
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

	function totalServerMbps() {
		return interfaces.reduce((total, iface) => total + (iface.server_mbps ?? 0), 0);
	}

	/** @param {number | null | undefined} value */
	function formatSeq(value) {
		return value == null ? '-' : String(value);
	}

	/** @param {InterfaceStatus} iface */
	function statusLabel(iface) {
		if (iface.status === 'connected') return 'Connected';
		if (iface.status === 'reconnecting') return 'Reconnecting';
		return 'Dead';
	}

	/** @param {InterfaceStatus} iface */
	function statusClasses(iface) {
		if (iface.status === 'connected') return 'bg-emerald-400 shadow-emerald-400/40';
		if (iface.status === 'reconnecting') return 'bg-amber-400 shadow-amber-400/40';
		return 'bg-red-500 shadow-red-500/40';
	}

	/** @param {Element} _node */
	function pollRemote(_node) {
		const statusInterval = setInterval(() => {
			if (polling) void readStatus();
		}, 1000);

		return () => {
			clearInterval(statusInterval);
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
						<p class="text-[0.6rem] font-bold text-neutral-600 uppercase">TX Mbps</p>
						<p class="text-sm font-black text-emerald-400">{totalInterfaceMbps().toFixed(2)}</p>
					</div>
					<div class="rounded-2xl bg-neutral-950 px-3 py-2">
						<p class="text-[0.6rem] font-bold text-neutral-600 uppercase">Srv Mbps</p>
						<p class="text-sm font-black text-sky-300">{totalServerMbps().toFixed(2)}</p>
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
				<div class="min-w-0">
					<p class="text-[0.6rem] font-bold text-neutral-500 uppercase">Auto strategy</p>
					<p class="truncate text-xl font-black tracking-[-0.04em]">{effectiveStrategy}</p>
				</div>
				<div class="rounded-2xl bg-emerald-400 px-3 py-2 text-xs font-black text-black">
					Auto
				</div>
			</section>

			<section class="grid gap-2">
				{#each interfaces as iface}
					<article class="rounded-3xl border border-white/10 bg-neutral-950 p-3">
						<div class="mb-3 grid grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
							<div class="flex min-w-0 items-center gap-2">
								<span class={['h-3 w-3 shrink-0 rounded-full shadow-[0_0_18px]', statusClasses(iface)]} title={statusLabel(iface)}></span>
								<h2 class="min-w-0 truncate text-xl font-black tracking-[-0.05em]">{iface.name}</h2>
							</div>
							<div class="grid grid-cols-2 gap-1.5">
								<div class="min-w-16 rounded-2xl bg-black px-2 py-1.5 text-right">
									<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">TX Mbps</p>
									<p class="text-xs font-black text-emerald-400">{formatMbps(iface.tx_mbps)}</p>
								</div>
								<div class="min-w-16 rounded-2xl bg-black px-2 py-1.5 text-right">
									<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">Srv Mbps</p>
									<p class="text-xs font-black text-sky-300">{formatMbps(iface.server_mbps)}</p>
								</div>
							</div>
						</div>

						<div class="grid grid-cols-3 gap-1.5">
							<div class="rounded-2xl bg-black px-2 py-1.5">
								<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">TX</p>
								<p class="text-xs font-black text-neutral-100">{formatBytes(iface.tx_bytes)}</p>
							</div>
							<div class="rounded-2xl bg-black px-2 py-1.5">
								<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">Last</p>
								<p class="text-xs font-black text-neutral-100">{formatSeq(iface.server_last_seq)}</p>
							</div>
							<div class="rounded-2xl bg-black px-2 py-1.5">
								<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">Max</p>
								<p class="text-xs font-black text-neutral-100">{formatSeq(iface.server_max_seq)}</p>
							</div>
						</div>
					</article>
				{/each}
			</section>
		{:else}
			<section class="rounded-3xl border border-white/10 bg-neutral-950 p-5">
				<p class="text-neutral-400">Connect to the irohsion BLE remote to monitor paths.</p>
			</section>
		{/if}
	</div>
</main>

<script>
	const SERVICE_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10001';
	const STATUS_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10002';
	const CONTROL_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10003';
	const STATUS_OFFSET_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10006';
	const OBS_SERVICE_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10011';
	const OBS_STATUS_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10012';
	const OBS_CONTROL_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10013';

	/** @typedef {{ connected?: boolean, connect: () => Promise<BluetoothRemoteGATTServer>, disconnect?: () => void }} BluetoothRemoteGATT */
	/** @typedef {{ getCharacteristic: (uuid: string) => Promise<BluetoothRemoteGATTCharacteristic> }} BluetoothRemoteGATTService */
	/** @typedef {{ readValue: () => Promise<DataView>, writeValue: (value: BufferSource) => Promise<void>, writeValueWithResponse?: (value: BufferSource) => Promise<void> }} BluetoothRemoteGATTCharacteristic */
	/** @typedef {{ connected?: boolean, disconnect?: () => void, getPrimaryService: (uuid: string) => Promise<BluetoothRemoteGATTService> }} BluetoothRemoteGATTServer */
	/** @typedef {{ name?: string, gatt: BluetoothRemoteGATT, addEventListener: (event: 'gattserverdisconnected', handler: () => void) => void }} BluetoothDevice */
	/** @typedef {{ requestDevice: (options: object) => Promise<BluetoothDevice> }} BluetoothApi */
	/** @typedef {{ name: string, status?: 'connected' | 'reconnecting' | 'dead', split_percentage?: number, tx_packets?: number, tx_bytes?: number, tx_mbps?: number, server_mbps?: number | null, server_last_seq?: number | null, server_max_seq?: number | null }} InterfaceStatus */
	/** @typedef {{ mode: string, effective_strategy: string, packets: number, payload_bytes: number, interfaces: InterfaceStatus[] }} ControlStatus */
	/** @typedef {{ enabled: boolean, decoding: boolean, jpeg_bytes: number, characteristic: string, offset_characteristic: string, chunk_bytes: number }} PreviewStatus */
	/** @typedef {{ id: number, unix_ms: number, user: string, text: string }} ChatMessage */
	/** @typedef {{ control: ControlStatus, preview?: PreviewStatus, chat?: ChatMessage[] }} RemoteStatus */
	/** @typedef {{ enabled: boolean, connected: boolean, host: string, port: number, last_error?: string | null, recording?: boolean | null, recording_bitrate_kbps?: number | null, video_fps?: number | null, recording_bitrate_category: string, recording_bitrate_name: string }} ObsStatus */

	/** @type {BluetoothDevice | null} */
	let device = $state(null);
	/** @type {BluetoothRemoteGATTServer | null} */
	let server = $state(null);
	/** @type {RemoteStatus | null} */
	let status = $state(null);
	/** @type {ObsStatus | null} */
	let obsStatus = $state(null);
	let error = $state('');
	let obsError = $state('');
	let obsBitrate = $state('6000');
	let editingObsBitrate = $state(false);
	let previewUrl = $state('');
	let previewBusy = false;
	let previewObjectUrl = '';
	let connecting = $state(false);
	let readingStatus = false;
	let obsBusy = $state(false);
	/** @type {Promise<unknown>} */
	let gattQueue = Promise.resolve();

	let connected = $derived(isGattConnected(server));
	let interfaces = $derived(getInterfaces(status));
	let effectiveStrategy = $derived(getEffectiveStrategy(status));
	let previewReady = $derived(isPreviewAvailable(status));
	let chatMessages = $derived(getChatMessages(status));

	/** @param {RemoteStatus | null} value */
	function getInterfaces(value) {
		return value?.control.interfaces ?? [];
	}

	/** @param {RemoteStatus | null} value */
	function getEffectiveStrategy(value) {
		return value?.control.effective_strategy ?? 'redundant';
	}

	/** @param {RemoteStatus | null} value */
	function isPreviewAvailable(value) {
		return Boolean(value && value.preview?.enabled);
	}

	/** @param {RemoteStatus | null} value */
	function getChatMessages(value) {
		return value?.chat ?? [];
	}

	/** @param {BluetoothRemoteGATTServer | null} value */
	function isGattConnected(value) {
		return Boolean(value && value.connected);
	}

	async function connect() {
		error = '';
		connecting = true;

		try {
			const bluetooth = /** @type {Navigator & { bluetooth?: BluetoothApi }} */ (navigator).bluetooth;
			if (!bluetooth) throw new Error('Web Bluetooth is not available in this browser.');

			const selectedDevice = await bluetooth.requestDevice({
				filters: [{ namePrefix: 'irohsion' }],
				optionalServices: [SERVICE_UUID, OBS_SERVICE_UUID]
			});
			device = selectedDevice;
			device.addEventListener('gattserverdisconnected', () => disconnectRemote('Bluetooth disconnected'));

			await reconnectGatt();
		} catch (err) {
			error = err instanceof Error ? err.message : String(err);
			disconnectRemote(error);
		} finally {
			connecting = false;
		}
	}

	async function reconnectGatt() {
		if (!device) return false;
		try {
			server = await device.gatt.connect();
			await readStatus();
			await readObsStatus();
			await readPreview();
			error = '';
			return true;
		} catch (err) {
			disconnectRemote(err instanceof Error ? err.message : String(err));
			return false;
		}
	}

	/** @param {string} reason */
	function disconnectRemote(reason = '') {
		try {
			disconnectGatt(server);
		} catch {
			// Browser GATT disconnect can throw if the underlying connection is already gone.
		}
		server = null;
		status = null;
		obsStatus = null;
		readingStatus = false;
		obsBusy = false;
		previewBusy = false;
		gattQueue = Promise.resolve();
		setPreviewUrl('');
		if (reason) error = reason;
	}

	async function ensureConnected() {
		if (isGattConnected(server)) return true;
		if (device && !connecting) return reconnectGatt();
		disconnectRemote('Bluetooth disconnected');
		return false;
	}

	/** @param {BluetoothRemoteGATTServer | null} value */
	function disconnectGatt(value) {
		if (value && value.disconnect) value.disconnect();
	}

	async function readStatus() {
		if (readingStatus || !(await ensureConnected())) return;
		readingStatus = true;

		try {
			await enqueueGatt(readStatusNow);
		} catch (err) {
			disconnectRemote(err instanceof Error ? err.message : String(err));
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

	/** @param {boolean} enabled */
	async function setPreviewEnabled(enabled) {
		if (!(await ensureConnected())) return;
		try {
			await enqueueGatt(async () => {
				const activeServer = server;
				if (!activeServer) return;
				const service = await retryGatt(() => activeServer.getPrimaryService(SERVICE_UUID));
				const characteristic = await retryGatt(() => service.getCharacteristic(CONTROL_UUID));
				await writeCharacteristic(characteristic, new TextEncoder().encode(JSON.stringify({ preview_enabled: enabled })));
			});
			await readStatus();
			await readPreview();
		} catch (err) {
			disconnectRemote(err instanceof Error ? err.message : String(err));
		}
	}

	async function readPreview() {
		if (previewBusy || !status?.preview?.enabled || !status.preview.decoding || status.preview.jpeg_bytes <= 0 || !(await ensureConnected())) return;
		previewBusy = true;
		try {
			await enqueueGatt(async () => {
				const activeServer = server;
				const preview = status?.preview;
				if (!activeServer || !preview) return;
				const service = await retryGatt(() => activeServer.getPrimaryService(SERVICE_UUID));
				const characteristic = await retryGatt(() => service.getCharacteristic(preview.characteristic));
				const offsetCharacteristic = await retryGatt(() => service.getCharacteristic(preview.offset_characteristic));
				const bytes = await readChunkedCharacteristic(characteristic, offsetCharacteristic, preview.jpeg_bytes);
				if (bytes.byteLength > 0) {
					setPreviewUrl(URL.createObjectURL(new Blob([bytes], { type: 'image/jpeg' })));
				}
			});
		} catch {
			// Preview is best-effort; status polling must keep working even if a frame read races ffmpeg.
		} finally {
			previewBusy = false;
		}
	}

	async function readObsStatus() {
		if (!(await ensureConnected())) return;
		try {
			await enqueueGatt(async () => {
				const activeServer = server;
				if (!activeServer) return;
				const service = await retryGatt(() => activeServer.getPrimaryService(OBS_SERVICE_UUID));
				const characteristic = await retryGatt(() => service.getCharacteristic(OBS_STATUS_UUID));
				const value = await readCharacteristic(characteristic);
				const text = new TextDecoder().decode(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
				obsStatus = JSON.parse(text);
				if (!editingObsBitrate) {
					obsBitrate = String(obsStatus?.recording_bitrate_kbps ?? obsBitrate);
				}
				obsError = '';
			});
		} catch (err) {
			obsStatus = null;
			obsError = err instanceof Error ? err.message : String(err);
		}
	}

	/** @param {object} command */
	async function sendObsCommand(command) {
		if (obsBusy || !(await ensureConnected())) return;
		obsBusy = true;
		obsError = '';
		try {
			await enqueueGatt(async () => {
				const activeServer = server;
				if (!activeServer) return;
				const service = await retryGatt(() => activeServer.getPrimaryService(OBS_SERVICE_UUID));
				const characteristic = await retryGatt(() => service.getCharacteristic(OBS_CONTROL_UUID));
				await writeCharacteristic(characteristic, new TextEncoder().encode(JSON.stringify(command)));
			});
			await readObsStatus();
		} catch (err) {
			obsError = err instanceof Error ? err.message : String(err);
			if (isGattDisconnectError(err)) disconnectRemote(obsError);
		} finally {
			obsBusy = false;
		}
	}

	function setObsBitrate() {
		const kbps = Number.parseInt(obsBitrate, 10);
		if (!Number.isFinite(kbps) || kbps <= 0) {
			obsError = 'Bitrate must be a positive number.';
			return;
		}
		editingObsBitrate = false;
		void sendObsCommand({ action: 'set_recording_bitrate', kbps });
	}

	/** @param {30 | 60} fps */
	function setObsFps(fps) {
		void sendObsCommand({ action: 'set_video_fps', fps });
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

	/** @param {unknown} err */
	function isGattDisconnectError(err) {
		const message = err instanceof Error ? err.message : String(err);
		return /gatt|bluetooth|disconnect|device|network|closed|not connected/i.test(message);
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

	/** @param {string | null | undefined} value */
	function strategyLabel(value) {
		if (value === 'split') return 'Split';
		if (value === 'roundrobin') return 'Round';
		return 'Redundant';
	}

	/** @param {string | null | undefined} value */
	function strategyClasses(value) {
		if (value === 'split') return 'bg-sky-300 text-black';
		if (value === 'roundrobin') return 'bg-fuchsia-300 text-black';
		return 'bg-amber-300 text-black';
	}

	/** @param {string} url */
	function setPreviewUrl(url) {
		if (previewObjectUrl) URL.revokeObjectURL(previewObjectUrl);
		previewObjectUrl = url;
		previewUrl = url;
	}

	/** @param {number | null | undefined} value */
	function formatSeq(value) {
		return value == null ? '-' : String(value);
	}

	/** @param {number | null | undefined} value */
	function formatSplit(value) {
		return `${Math.round(value ?? 0)}%`;
	}

	/** @param {number | null | undefined} unixMs */
	function formatChatTime(unixMs) {
		if (!unixMs) return '';
		return new Date(unixMs).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
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
			void readStatus();
			void readObsStatus();
			void readPreview();
		}, 1000);
		const recover = () => {
			if (!document.hidden) void recoverConnection();
		};
		document.addEventListener('visibilitychange', recover);
		window.addEventListener('focus', recover);

		return () => {
			clearInterval(statusInterval);
			document.removeEventListener('visibilitychange', recover);
			window.removeEventListener('focus', recover);
		};
	}

	async function recoverConnection() {
		if (isGattConnected(server)) {
			void readStatus();
			void readObsStatus();
			void readPreview();
			return;
		}
		if (device && !connecting) await reconnectGatt();
	}

	async function forceUpdate() {
		error = '';
		try {
			if ('serviceWorker' in navigator) {
				const registrations = await navigator.serviceWorker.getRegistrations();
				await Promise.all(registrations.map((registration) => registration.unregister()));
			}
			if ('caches' in window) {
				const keys = await caches.keys();
				await Promise.all(keys.map((key) => caches.delete(key)));
			}
		} catch (err) {
			error = err instanceof Error ? err.message : String(err);
		} finally {
			const url = new URL(window.location.href);
			url.searchParams.set('update', String(Date.now()));
			window.location.replace(url.toString());
		}
	}

	/** @param {Element} node */
	function scrollChatToBottom(node) {
		$effect(() => {
			chatMessages.length;
			setTimeout(() => {
				node.scrollTop = node.scrollHeight;
			}, 0);
		});
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
				<span
					class={[
						'h-3.5 w-3.5 rounded-full',
						obsStatus?.connected ? 'bg-emerald-400 shadow-[0_0_18px_rgba(52,211,153,0.9)]' : 'bg-red-500'
					]}
					title={obsStatus?.connected ? 'OBS connected' : 'OBS disconnected'}
					aria-label={obsStatus?.connected ? 'OBS connected' : 'OBS disconnected'}
				></span>
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
			<section {@attach pollRemote} class="overflow-hidden rounded-3xl border border-white/10 bg-neutral-950">
				<div class="aspect-video bg-black">
					{#if previewUrl}
						<img class="h-full w-full object-contain" src={previewUrl} alt="Decoded stream preview" />
					{:else}
						<div class="grid h-full place-items-center px-4 text-center text-sm font-bold text-neutral-600">
							{previewReady ? 'Waiting for preview frame' : 'Preview disabled'}
						</div>
					{/if}
				</div>
				<div class="flex items-center justify-between gap-2 p-2">
					<div>
						<p class="text-[0.6rem] font-bold text-neutral-600 uppercase">Frame preview</p>
						<p class="text-xs font-bold text-neutral-400">
							{status.preview?.decoding ? `${status.preview.jpeg_bytes ?? 0} bytes` : 'decoder idle'}
						</p>
					</div>
					<button
						class={[
							'rounded-2xl px-3 py-2 text-xs font-black',
							status.preview?.decoding ? 'bg-emerald-400 text-black' : 'bg-neutral-800 text-neutral-300'
						]}
						onclick={() => setPreviewEnabled(!status?.preview?.decoding)}
					>
						{status.preview?.decoding ? 'Preview On' : 'Preview Off'}
					</button>
				</div>
			</section>

			<section class="rounded-3xl border border-white/10 bg-neutral-950 p-3">
				<div class="mb-2 flex items-center justify-between gap-3">
					<div>
						<p class="text-[0.6rem] font-bold text-neutral-500 uppercase">Twitch Chat</p>
						<p class="text-xl font-black tracking-[-0.05em]">{chatMessages.length} messages</p>
					</div>
				</div>

				<div {@attach scrollChatToBottom} class="grid max-h-72 gap-2 overflow-y-auto pr-1">
					{#if chatMessages.length}
						{#each chatMessages as message (message.id)}
							<article class="rounded-2xl bg-black px-3 py-2">
								<div class="mb-1 flex items-baseline justify-between gap-2">
									<p class="min-w-0 truncate text-sm font-black text-sky-300">{message.user}</p>
									<p class="shrink-0 text-[0.6rem] font-bold text-neutral-600">{formatChatTime(message.unix_ms)}</p>
								</div>
								<p class="break-words text-sm font-semibold text-neutral-100">{message.text}</p>
							</article>
						{/each}
					{:else}
						<p class="rounded-2xl bg-black px-3 py-4 text-sm font-bold text-neutral-600">
							No chat messages
						</p>
					{/if}
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
								<div class="grid grid-cols-[auto_auto_auto_auto] gap-1.5">
									<div class={['rounded-2xl px-2 py-1.5 text-right', strategyClasses(effectiveStrategy)]}>
										<p class="text-[0.55rem] font-black uppercase opacity-60">Mode</p>
										<p class="text-xs font-black">{strategyLabel(effectiveStrategy)}</p>
									</div>
									<div class="min-w-14 rounded-2xl bg-black px-2 py-1.5 text-right">
										<p class="text-[0.55rem] font-bold text-neutral-600 uppercase">Split</p>
										<p class="text-xs font-black text-amber-300">{formatSplit(iface.split_percentage)}</p>
									</div>
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

			<section class="rounded-3xl border border-white/10 bg-neutral-950 p-3">
				<div class="mb-3 flex items-center justify-between gap-3">
					<div>
						<p class="text-[0.6rem] font-bold text-neutral-500 uppercase">OBS Recording</p>
						<p class="text-xl font-black tracking-[-0.05em]">
							{obsStatus?.recording ? 'Recording' : obsStatus?.connected ? 'Ready' : 'Disconnected'}
						</p>
					</div>
					<span
						class={[
							'h-3 w-3 rounded-full shadow-[0_0_18px]',
							obsStatus?.recording
								? 'bg-red-500 shadow-red-500/50'
								: obsStatus?.connected
									? 'bg-emerald-400 shadow-emerald-400/40'
									: 'bg-neutral-700'
						]}
					></span>
				</div>

				<div class="grid grid-cols-2 gap-2">
					<button
						class="rounded-2xl bg-red-500 px-3 py-3 text-sm font-black text-white disabled:opacity-40"
						disabled={!server || obsBusy}
						onclick={() => sendObsCommand({ action: 'start_record' })}
					>
						Start Rec
					</button>
					<button
						class="rounded-2xl bg-neutral-800 px-3 py-3 text-sm font-black text-white disabled:opacity-40"
						disabled={!server || obsBusy}
						onclick={() => sendObsCommand({ action: 'stop_record' })}
					>
						Stop Rec
					</button>
					</div>

					<div class="mt-2 rounded-2xl bg-black px-3 py-2">
						<p class="mb-2 text-[0.55rem] font-bold text-neutral-600 uppercase">Video FPS</p>
						<div class="grid grid-cols-2 gap-1.5">
							<button
								class={[
									'rounded-xl px-3 py-2 text-sm font-black disabled:opacity-40',
									(obsStatus?.video_fps ?? 0) === 30 ? 'bg-sky-300 text-black' : 'bg-neutral-800 text-neutral-200'
								]}
								disabled={!server || obsBusy}
								onclick={() => setObsFps(30)}
							>
								30
							</button>
							<button
								class={[
									'rounded-xl px-3 py-2 text-sm font-black disabled:opacity-40',
									(obsStatus?.video_fps ?? 0) === 60 ? 'bg-sky-300 text-black' : 'bg-neutral-800 text-neutral-200'
								]}
								disabled={!server || obsBusy}
								onclick={() => setObsFps(60)}
							>
								60
							</button>
						</div>
					</div>

					<div class="mt-2 grid grid-cols-[minmax(0,1fr)_auto] gap-2">
					<label class="rounded-2xl bg-black px-3 py-2">
						<span class="block text-[0.55rem] font-bold text-neutral-600 uppercase">Recording bitrate kbps</span>
						<input
							class="w-full bg-transparent text-lg font-black text-neutral-100 outline-none"
							inputmode="numeric"
							bind:value={obsBitrate}
							onfocus={() => (editingObsBitrate = true)}
							oninput={() => (editingObsBitrate = true)}
							onblur={() => (editingObsBitrate = false)}
						/>
					</label>
					<button
						class="rounded-2xl bg-sky-300 px-4 py-2 text-sm font-black text-black disabled:opacity-40"
						disabled={!server || obsBusy}
						onclick={setObsBitrate}
					>
						Set
					</button>
				</div>

				{#if obsStatus}
					<p class="mt-2 text-[0.65rem] font-bold text-neutral-600">
						{obsStatus.host}:{obsStatus.port} · {obsStatus.recording_bitrate_category}/{obsStatus.recording_bitrate_name}
					</p>
				{/if}
				{#if obsError}
					<p class="mt-2 text-xs font-bold text-amber-300">{obsError}</p>
				{/if}
			</section>

		{:else}
			<section class="rounded-3xl border border-white/10 bg-neutral-950 p-5">
				<p class="text-neutral-400">Connect to the irohsion BLE remote to monitor paths.</p>
			</section>
			<div class="sticky bottom-[max(0.5rem,env(safe-area-inset-bottom))]">
				<button
					class="w-full rounded-2xl border border-white/10 bg-neutral-900 px-4 py-3 text-sm font-black text-neutral-100 active:scale-[0.99]"
					onclick={forceUpdate}
				>
					Force Update
				</button>
			</div>
		{/if}
	</div>
</main>

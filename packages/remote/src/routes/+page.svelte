<script>
	const SERVICE_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10001';
	const STATUS_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10002';
	const CONTROL_UUID = '8b4f82c8-4f5a-4e26-8f29-d1f0c0d10003';

	/** @typedef {{ connect: () => Promise<BluetoothRemoteGATTServer> }} BluetoothRemoteGATT */
	/** @typedef {{ getCharacteristic: (uuid: string) => Promise<BluetoothRemoteGATTCharacteristic> }} BluetoothRemoteGATTService */
	/** @typedef {{ readValue: () => Promise<DataView>, writeValue: (value: BufferSource) => Promise<void> }} BluetoothRemoteGATTCharacteristic */
	/** @typedef {{ getPrimaryService: (uuid: string) => Promise<BluetoothRemoteGATTService> }} BluetoothRemoteGATTServer */
	/** @typedef {{ name?: string, gatt: BluetoothRemoteGATT, addEventListener: (event: 'gattserverdisconnected', handler: () => void) => void }} BluetoothDevice */
	/** @typedef {{ requestDevice: (options: object) => Promise<BluetoothDevice> }} BluetoothApi */
	/** @typedef {{ name: string, target_mbps?: number | null, split_percentage?: number | null }} InterfaceStatus */
	/** @typedef {{ mode: string, effective_strategy: string, packets: number, payload_bytes: number, interfaces: InterfaceStatus[] }} ControlStatus */
	/** @typedef {{ control: ControlStatus }} RemoteStatus */
	/** @typedef {{ mode?: string, targets_mbps?: Record<string, number>, split_percentages?: Record<string, number> }} ControlPatch */

	/** @type {BluetoothDevice | null} */
	let device = $state(null);
	/** @type {BluetoothRemoteGATTServer | null} */
	let server = $state(null);
	/** @type {RemoteStatus | null} */
	let status = $state(null);
	let error = $state('');
	let connecting = $state(false);
	let saving = $state(false);

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
		if (!server) return;

		const service = await server.getPrimaryService(SERVICE_UUID);
		const characteristic = await service.getCharacteristic(STATUS_UUID);
		const value = await characteristic.readValue();
		status = JSON.parse(new TextDecoder().decode(value));
	}

	/** @param {ControlPatch} patch */
	async function writePatch(patch) {
		if (!server) return;
		saving = true;
		error = '';

		try {
			const service = await server.getPrimaryService(SERVICE_UUID);
			const characteristic = await service.getCharacteristic(CONTROL_UUID);
			await characteristic.writeValue(new TextEncoder().encode(JSON.stringify(patch)));
			await readStatus();
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

	/** @param {InterfaceStatus} iface */
	function displayPercentage(iface) {
		return iface.split_percentage ?? evenPercentage();
	}

	function evenPercentage() {
		return interfaces.length > 0 ? Math.round(100 / interfaces.length) : 0;
	}
</script>

<svelte:head>
	<title>Irohsion Remote</title>
</svelte:head>

<main class="min-h-svh bg-black px-4 py-3 text-neutral-100">
	<div class="mx-auto grid max-w-xl gap-4">
		<header class="flex items-center justify-between gap-3">
			<button
				class="flex items-center gap-3 rounded-full border border-white/10 bg-neutral-950 px-4 py-3 text-left active:scale-[0.99]"
				onclick={connect}
				disabled={connecting}
			>
				<span
					class={[
						'h-3.5 w-3.5 rounded-full',
						connected ? 'bg-emerald-400 shadow-[0_0_18px_rgba(52,211,153,0.9)]' : 'bg-red-500'
					]}
				></span>
				<span class="text-sm font-black">{connecting ? 'Connecting...' : connected ? 'Connected' : 'Connect'}</span>
			</button>

			<div class="min-w-0 text-right">
				<p class="truncate text-sm font-bold">{device?.name ?? 'No device'}</p>
				<p class="text-xs text-neutral-500">irohsion remote</p>
			</div>
		</header>

		{#if error}
			<section class="rounded-3xl border border-red-500/30 bg-red-950/30 p-4 text-sm text-red-100">
				{error}
			</section>
		{/if}

		{#if status}
			<section class="rounded-3xl border border-white/10 bg-neutral-950 p-4">
				<div class="mb-4 flex items-center justify-between">
					<div>
						<p class="text-xs font-black tracking-[0.18em] text-neutral-500 uppercase">Strategy</p>
						<p class="text-2xl font-black tracking-[-0.04em]">{effectiveStrategy}</p>
					</div>
					<button class="rounded-full bg-white px-4 py-2 text-sm font-black text-black" onclick={readStatus}>
						Refresh
					</button>
				</div>

				<div class="grid grid-cols-3 gap-2">
					{#each ['auto', 'split', 'redundant'] as option}
						<button
							class={[
								'rounded-2xl px-3 py-3 text-sm font-black capitalize',
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

			<section class="grid gap-3">
				{#each interfaces as iface}
					<article class="rounded-3xl border border-white/10 bg-neutral-950 p-4">
						<div class="mb-4 flex items-baseline justify-between gap-4">
							<h2 class="text-2xl font-black tracking-[-0.05em]">{iface.name}</h2>
							<p class="text-sm text-neutral-500">{iface.target_mbps ?? 0} Mbps</p>
						</div>

						<label class="grid gap-2">
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
							<label class="mt-4 grid gap-2">
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
				<p class="text-xs font-black tracking-[0.18em] text-neutral-500 uppercase">Counters</p>
				<div class="mt-3 grid grid-cols-2 gap-3">
					<div class="rounded-2xl bg-black p-3">
						<p class="text-xs text-neutral-500">Packets</p>
						<p class="text-2xl font-black">{status.control?.packets ?? 0}</p>
					</div>
					<div class="rounded-2xl bg-black p-3">
						<p class="text-xs text-neutral-500">Payload MB</p>
						<p class="text-2xl font-black">{(((status.control?.payload_bytes ?? 0) / 1_000_000).toFixed(2))}</p>
					</div>
				</div>
			</section>
		{:else}
			<section class="rounded-3xl border border-white/10 bg-neutral-950 p-5">
				<p class="text-neutral-400">Connect to the irohsion BLE remote to tune paths.</p>
			</section>
		{/if}
	</div>
</main>

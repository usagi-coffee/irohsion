<script>
	import { base } from '$app/paths';
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';

	let { children } = $props();

	function registerServiceWorker() {
		if (!('serviceWorker' in navigator)) return;
		navigator.serviceWorker.register(`${base}/service-worker.js`).catch(() => {});
	}
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>
<svelte:window onload={registerServiceWorker} />
{@render children()}

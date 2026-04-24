import { $ } from 'bun';

await $`bun run build`
await $`rsync -a -v -r build/* chiya:/var/www/irohsion/`;


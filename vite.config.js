import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// 后端端口可通过 ROUNDTABLE_PORT 覆盖；默认 3000。
// 注意：部分 Windows 机器的 3000 落在 Hyper-V/WSL 保留端口段而无法监听，
// 此时后端与本代理需同步改到可用端口（如 4500）。
const BACKEND_PORT = process.env.ROUNDTABLE_PORT || '3000';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': `http://localhost:${BACKEND_PORT}`,
      '/ws': {
        target: `ws://localhost:${BACKEND_PORT}`,
        ws: true,
      },
    },
  },
});

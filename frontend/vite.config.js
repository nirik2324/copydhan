import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // Proxy API calls to FastAPI in dev
      '/api': 'http://localhost:8000',
      // Proxy WebSocket to FastAPI in dev
      '/ws': {
        target: 'ws://localhost:8000',
        ws: true,
      },
    },
  },
  build: {
    outDir: '../static',
    emptyOutDir: true,
  },
})

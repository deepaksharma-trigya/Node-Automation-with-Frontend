import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Proxy all backend calls to catalyst serve
      // catalyst serve exposes functions at /server/{functionName}/
      '/server/backend': {
        target:       'http://localhost:3000',
        changeOrigin: true,
        // Keep the /server/backend prefix — catalyst needs it for routing
      },
    },
  },
})
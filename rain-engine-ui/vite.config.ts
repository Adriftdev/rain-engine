import { defineConfig } from 'vite'
import solid from 'vite-plugin-solid'
import path from 'path'

export default defineConfig({
  plugins: [solid()],
  resolve: {
    alias: {
      '~': path.resolve(__dirname, './src'),
      'styled-system': path.resolve(__dirname, './styled-system'),
    },
  },
  server: {
    proxy: {
      // Proxy requests to the Gateway
      '/api': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
      // Also proxy SSE
      '/stream': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: true,
        // don't rewrite if the gateway expects /sessions/{id}/stream
      }
    }
  }
})

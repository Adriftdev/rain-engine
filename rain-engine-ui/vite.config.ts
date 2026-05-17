import { defineConfig } from 'vite'
import solid from 'vite-plugin-solid'
import path from 'path'
import commonjs from 'vite-plugin-commonjs'

export default defineConfig({
  plugins: [
    solid(),
    commonjs()
  ],
  resolve: {
    alias: {
      '~': path.resolve(__dirname, './src'),
      'styled-system': path.resolve(__dirname, './styled-system'),
    },
  },
  optimizeDeps: {
    include: ['solid-markdown', 'debug', 'extend'],
    exclude: ['@tanstack/solid-virtual']
  },
  build: {
    target: 'esnext',
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

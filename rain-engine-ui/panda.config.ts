import { defineConfig } from '@pandacss/dev'
import { createPreset } from '@park-ui/panda-preset'
import indigo from '@park-ui/panda-preset/colors/indigo'
import slate from '@park-ui/panda-preset/colors/slate'

export default defineConfig({
  // Whether to use css reset
  preflight: true,

  // Where to look for your css declarations
  include: ['./src/**/*.{js,jsx,ts,tsx}', './pages/**/*.{js,jsx,ts,tsx}'],

  // Files to exclude
  exclude: [],

  // The output directory for your css system
  outdir: 'styled-system',

  // Park UI preset with indigo accent + slate gray + 2xl corners
  presets: [
    '@pandacss/preset-panda',
    createPreset({
      accentColor: indigo,
      grayColor: slate,
      borderRadius: '2xl',
    }),
  ],

  theme: {
    extend: {
      semanticTokens: {
        colors: {
          bg: {
            canvas: { value: '{colors.slate.950}' },
            sidebar: { value: 'hsla(240, 10%, 2%, 0.8)' },
            card: { value: '{colors.slate.900}' },
          },
        },
      },
      keyframes: {
        pulse: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '.5' },
        },
        fadeIn: {
          '0%': { opacity: '0', transform: 'translateY(5px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
      },
    },
  },

  // JSX framework support
  jsxFramework: 'solid',
})


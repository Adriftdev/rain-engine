import { ark } from '@ark-ui/solid'
import { styled } from 'styled-system/jsx'
import type { ComponentProps } from 'solid-js'

export type InputProps = ComponentProps<typeof ark.input>
export const Input = styled(ark.input, {
  base: {
    display: 'inline-flex',
    alignItems: 'center',
    px: '3',
    py: '2',
    bg: 'bg.default',
    border: '1px solid',
    borderColor: 'border.default',
    borderRadius: 'md',
    outline: 'none',
    _focus: {
      borderColor: 'accent.default',
      boxShadow: '0 0 0 1px var(--colors-accent-default)'
    },
    _disabled: {
      opacity: 0.5,
      cursor: 'not-allowed'
    }
  }
})

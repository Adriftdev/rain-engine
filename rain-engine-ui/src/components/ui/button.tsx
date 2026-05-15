import { ark } from '@ark-ui/solid'
import { styled } from 'styled-system/jsx'
import type { ComponentProps } from 'solid-js'

export type ButtonProps = ComponentProps<typeof ark.button>
export const Button = styled(ark.button, {
  base: {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    px: '4',
    py: '2',
    bg: 'accent.default',
    color: 'accent.fg',
    borderRadius: 'md',
    fontWeight: 'medium',
    cursor: 'pointer',
    _hover: {
      bg: 'accent.emphasized'
    },
    _disabled: {
      opacity: 0.5,
      cursor: 'not-allowed'
    }
  }
})

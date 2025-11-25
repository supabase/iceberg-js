export interface IcebergErrorResponse {
  error: {
    message: string
    type: string
    code: number
    stack?: string[]
  }
}

export class IcebergError extends Error {
  readonly status: number
  readonly icebergType?: string
  readonly icebergCode?: number
  readonly details?: unknown
  readonly isCommitStateUnknown: boolean

  constructor(
    message: string,
    opts: {
      status: number
      icebergType?: string
      icebergCode?: number
      details?: unknown
    }
  ) {
    super(message)
    this.name = 'IcebergError'
    this.status = opts.status
    this.icebergType = opts.icebergType
    this.icebergCode = opts.icebergCode
    this.details = opts.details

    // Detect CommitStateUnknownException (500, 502, 504 during table commits)
    this.isCommitStateUnknown =
      opts.icebergType === 'CommitStateUnknownException' ||
      ([500, 502, 504].includes(opts.status) && opts.icebergType?.includes('CommitState') === true)
  }

  /**
   * Returns true if the error is a 404 Not Found error.
   */
  isNotFound(): boolean {
    return this.status === 404
  }

  /**
   * Returns true if the error is a 409 Conflict error.
   */
  isConflict(): boolean {
    return this.status === 409
  }

  /**
   * Returns true if the error is a 419 Authentication Timeout error.
   */
  isAuthenticationTimeout(): boolean {
    return this.status === 419
  }
}

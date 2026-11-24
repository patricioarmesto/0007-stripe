// El frontend solo habla con /api: nginx es la puerta de entrada.
// Nunca ve los secretos ni los puertos internos de los binarios.
export type Payment = {
  id: string
  amount: number
  currency: string
  email: string
  status: string
  created_at: string
  attempts: number
}

export type CreateResult = { payment: Payment; created: boolean }

export type DayTotal = { day: string; total: number }
export type StatusCount = { status: string; count: number }

const jsonHeaders = { 'Content-Type': 'application/json' }

export async function createPayment(
  input: { amount: number; email: string },
  idempotencyKey: string = crypto.randomUUID(),
): Promise<CreateResult> {
  const res = await fetch('/api/payments', {
    method: 'POST',
    headers: { ...jsonHeaders, 'Idempotency-Key': idempotencyKey },
    body: JSON.stringify({
      amount: input.amount,
      currency: 'ARS',
      email: input.email,
      payment_method: 'card_test',
    }),
  })
  if (!res.ok) throw new Error(`POST /payments: ${res.status}`)
  return res.json()
}

export async function simulateWebhook(paymentId: string) {
  const res = await fetch('/api/webhooks/simulate', {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify({ payment_id: paymentId, type: 'payment.succeeded' }),
  })
  if (!res.ok) throw new Error(`simulate: ${res.status}`)
  return res.json() as Promise<{ processed: boolean; event_id?: string }>
}

export async function retryWebhook(input: {
  payment_id: string
  event_id: string
  type: string
}) {
  const res = await fetch('/api/webhooks/retry', {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify(input),
  })
  return { status: res.status, body: await res.json() }
}

export async function listPayments(): Promise<Payment[]> {
  const res = await fetch('/api/payments')
  if (!res.ok) throw new Error(`list: ${res.status}`)
  const data = await res.json()
  return data.payments
}

export async function fetchStats(): Promise<{
  dias: DayTotal[]
  estados: StatusCount[]
}> {
  const [dias, estados] = await Promise.all([
    fetch('/api/stats?total-by-day').then((r) => r.json()),
    fetch('/api/stats?by-status').then((r) => r.json()),
  ])
  return { dias, estados }
}

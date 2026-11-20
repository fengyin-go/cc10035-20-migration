/** 输出分发：迁移清单、真实格式、字段契约回执与读回结果。 */
import { request } from './client'

export type FormatReadback = {
  ok: boolean
  config_id: number
  version: number
  requested_format: string
  declared_format: string
  actual_format: string
  body_sniff?: string
  read_format_used?: string
  fallback_used?: boolean
  declared_matches_actual: boolean
  readable_by_declared: boolean
  schema_version: string
  row_count?: number
  columns?: string[]
  null_counts?: Record<string, number>
  records?: Record<string, unknown>[]
  reason?: string
  warning?: string
}

export type SchemaReceipt = {
  switched_fields: string[]
  kept_fields: string[]
  pending_fields: string[]
  legacy_only_fields: string[]
  columns: {
    name: string
    type: string
    source_field: string
    legacy_field: string
    legacy_key: string
    nullable: boolean
    default: string
    contract_state: string
    switched: boolean
    type_changed: boolean
  }[]
}

export type TargetVersion = {
  version: number
  declared_format: string
  actual_format: string
  schema_version: string
  row_count: number
  size: number
  created_at: string
  delivery_status: string
  readable_by_declared: boolean | null
  readable_by_actual: boolean | null
  declared_matches_actual: boolean
  conclusion: Record<string, unknown> | null
  note: string
  is_current: boolean
}

export type ManifestTarget = {
  config_id: number
  config_name: string
  target: string
  declared_format: string
  schema_version: string
  align_rule_id: number | null
  current_version: number | null
  current_actual_format: string | null
  current_declared_matches_actual: boolean | null
  versions: TargetVersion[]
}

export type Manifest = {
  ok: boolean
  total_targets: number
  targets: ManifestTarget[]
  summary: { migrated: number; mismatched: number }
}

export type OverviewTarget = {
  config_id: number
  config_name: string
  declared_format: string
  target: string
  schema_version: string
  align_rule_id: number | null
  current_version: number | null
  current_actual_format: string | null
  declared_matches_actual: boolean | null
  delivery_status: string
  latest_version: number | null
  latest_status: string | null
  latest_conclusion: Record<string, unknown> | null
  schema_receipt: SchemaReceipt | null
  version_count: number
}

export type DistributionOverview = {
  ok: boolean
  targets: OverviewTarget[]
  summary: { migrated: number; mismatched: number }
}

export type MigrationResult = {
  ok: boolean
  status?: string
  reason?: string
  error?: string
  version?: number
  previous_version?: number | null
  declared_format?: string
  schema_version?: string
  row_count?: number
  columns?: string[]
  dry_run?: boolean
  verification?: {
    ok: boolean
    format?: string
    row_count?: number
    columns?: string[]
    null_counts?: Record<string, number>
    issues?: string[]
  }
  legacy_verification?: { ok: boolean; row_count?: number; columns?: string[] }
  backend_readback?: {
    available: boolean
    ok: boolean | null
    reason?: string
    schema?: { name: string; type: string }[]
    row_count?: number
  }
  schema_receipt?: SchemaReceipt
  conclusion?: Record<string, unknown> | null
}

export type RollbackResult = {
  ok: boolean
  reason?: string
  previous_version?: number | null
  current_version?: number
  declared_format?: string
  actual_format?: string
  declared_matches_actual?: boolean
  row_count?: number
  readback_format?: string
  fallback_used?: boolean
  warning?: string
}

async function readJson<T>(response: Response, fallback: string): Promise<T> {
  if (!response.ok) throw new Error(`${fallback}（HTTP ${response.status}）`)
  return (await response.json()) as T
}

export async function loadDistributionOverview(): Promise<DistributionOverview> {
  return readJson(await request('/api/output_target/distribution/overview'), '分发总览读取失败')
}

export async function loadManifest(): Promise<Manifest> {
  return readJson(await request('/api/output_target/distribution/manifest'), '迁移清单读取失败')
}

export async function readDistributionVersion(
  configId: number,
  options: { version?: number; format?: string; fallback?: boolean } = {},
): Promise<FormatReadback> {
  const query = new URLSearchParams()
  if (options.version !== undefined) query.set('version', String(options.version))
  if (options.format) query.set('format', options.format)
  if (options.fallback) query.set('fallback', 'true')
  const suffix = query.toString() ? `?${query}` : ''
  return readJson(
    await request(`/api/output_target/${configId}/distribution/read${suffix}`),
    '版本读回失败',
  )
}

export async function runDistributionMigration(
  configId: number,
  dryRun: boolean,
): Promise<MigrationResult> {
  return readJson(
    await request(`/api/output_target/${configId}/distribution/run`, {
      method: 'POST',
      body: JSON.stringify({ dry_run: dryRun }),
    }),
    dryRun ? '迁移试跑失败' : '迁移切换失败',
  )
}

export async function rollbackDistribution(
  configId: number,
  toVersion?: number,
): Promise<RollbackResult> {
  return readJson(
    await request(`/api/output_target/${configId}/distribution/rollback`, {
      method: 'POST',
      body: JSON.stringify(toVersion === undefined ? {} : { to_version: toVersion }),
    }),
    '回滚失败',
  )
}

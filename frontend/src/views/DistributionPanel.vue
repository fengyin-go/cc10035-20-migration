<template>
  <section class="distribution" data-testid="distribution-panel">
    <header class="dist-head">
      <div>
        <h3>输出分发与迁移清单</h3>
        <p class="page-desc">
          每个目标按声明的格式重建实体分发数据集，读回核对后把结论写回清单；
          存量“声明 Parquet、正文 JSON”的版本按实际格式如实标注。
        </p>
      </div>
      <div class="page-actions">
        <span class="dist-summary" data-testid="dist-summary">
          已对齐 {{ summary.migrated }} / 不一致 {{ summary.mismatched }}
        </span>
        <button class="btn" data-testid="reload-distribution" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="dist-error">{{ errorMessage }}</p>
    <p v-if="notice" class="dist-notice" data-testid="dist-notice">{{ notice }}</p>

    <div class="dist-grid">
      <article
        v-for="target in targets"
        :key="target.config_id"
        class="dist-card"
        :data-testid="`dist-target-${target.config_id}`"
        :data-matches="String(target.declared_matches_actual)"
      >
        <header class="dist-card-head">
          <div>
            <strong>{{ target.config_name }}</strong>
            <span class="dist-addr">{{ target.target }}</span>
          </div>
          <span class="dist-badge" :class="badgeClass(target)">
            {{ target.delivery_status }}
          </span>
        </header>

        <dl class="dist-facts">
          <div>
            <dt>声明格式</dt>
            <dd data-testid="declared-format">{{ target.declared_format }}</dd>
          </div>
          <div>
            <dt>真实正文格式</dt>
            <dd data-testid="actual-format">{{ target.current_actual_format ?? '—' }}</dd>
          </div>
          <div>
            <dt>字段版本</dt>
            <dd data-testid="schema-version">{{ target.schema_version || '—' }}</dd>
          </div>
          <div>
            <dt>当前版本</dt>
            <dd data-testid="current-version">v{{ target.current_version ?? '—' }}</dd>
          </div>
          <div>
            <dt>交付结果</dt>
            <dd data-testid="delivery-result">
              <template v-if="target.declared_matches_actual">
                声明与正文一致
              </template>
              <template v-else>
                声明与正文不一致
              </template>
            </dd>
          </div>
          <div>
            <dt>留存版本</dt>
            <dd>{{ target.version_count }}</dd>
          </div>
        </dl>

        <div v-if="target.schema_receipt" class="dist-receipt" data-testid="schema-receipt">
          <p><span class="dist-label">已切换字段</span>
            {{ listOrDash(target.schema_receipt.switched_fields) }}</p>
          <p><span class="dist-label">沿用旧列名</span>
            {{ listOrDash(target.schema_receipt.kept_fields) }}</p>
          <p v-if="target.schema_receipt.pending_fields.length">
            <span class="dist-label">待补契约</span>
            {{ listOrDash(target.schema_receipt.pending_fields) }}
          </p>
        </div>

        <div class="dist-actions">
          <button
            class="btn"
            type="button"
            :data-testid="`dry-run-${target.config_id}`"
            :disabled="busy === target.config_id"
            @click="migrate(target, true)"
          >
            试跑
          </button>
          <button
            class="btn primary"
            type="button"
            :data-testid="`migrate-${target.config_id}`"
            :disabled="busy === target.config_id"
            @click="migrate(target, false)"
          >
            切换交付
          </button>
          <button
            class="btn"
            type="button"
            :data-testid="`read-${target.config_id}`"
            :disabled="busy === target.config_id"
            @click="readBack(target, target.declared_format)"
          >
            按声明格式读回
          </button>
          <button
            class="btn ghost"
            type="button"
            :data-testid="`rollback-${target.config_id}`"
            :disabled="busy === target.config_id"
            @click="rollback(target)"
          >
            回滚上一版
          </button>
        </div>

        <details class="dist-versions">
          <summary>版本流水（{{ target.version_count }}）</summary>
          <table class="data-table">
            <thead>
              <tr>
                <th>版本</th><th>声明</th><th>正文</th><th>字段版本</th>
                <th>行数</th><th>状态</th><th>读回</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="version in versionsOf(target.config_id)"
                :key="version.version"
                :data-testid="`version-${target.config_id}-${version.version}`"
                :data-current="String(version.is_current)"
              >
                <td>v{{ version.version }}</td>
                <td>{{ version.declared_format }}</td>
                <td>{{ version.actual_format }}</td>
                <td>{{ version.schema_version }}</td>
                <td>{{ version.row_count }}</td>
                <td>{{ version.delivery_status }}</td>
                <td>
                  <button
                    class="link"
                    type="button"
                    :data-testid="`read-version-${target.config_id}-${version.version}`"
                    @click="readBack(target, version.actual_format, version.version)"
                  >
                    按实际格式
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </details>
      </article>
    </div>

    <section v-if="readback" class="dist-readback" data-testid="dist-readback">
      <header class="dist-readback-head">
        <strong>
          读回结果 · 目标 {{ readback.config_id }} · v{{ readback.version }} ·
          {{ readback.read_format_used || readback.requested_format }}
        </strong>
        <button class="btn ghost" type="button" @click="readback = null">关闭</button>
      </header>
      <p v-if="readback.warning" class="dist-notice">{{ readback.warning }}</p>
      <p>
        <span class="dist-label">声明格式</span> {{ readback.declared_format }}
        <span class="dist-label">真实正文</span> {{ readback.body_sniff || readback.actual_format }}
        <span class="dist-label">结果</span>
        {{ readback.ok ? '读取成功' : '读取失败' }}
        <template v-if="readback.ok">
          <span class="dist-label">行数</span> {{ readback.row_count }}
        </template>
        <span v-else class="error-text">{{ readback.reason }}</span>
      </p>
      <p v-if="readback.ok">
        <span class="dist-label">列</span> {{ (readback.columns || []).join('、') }}
        <span class="dist-label">空值</span>
        {{ nullSummary(readback.null_counts) }}
      </p>
      <table v-if="readback.ok && readback.records?.length" class="data-table">
        <thead>
          <tr><th v-for="column in readback.columns" :key="column">{{ column }}</th></tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in readback.records" :key="index">
            <td v-for="column in readback.columns" :key="column">
              {{ row[column] === null ? 'NULL' : row[column] }}
            </td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  loadDistributionOverview,
  loadManifest,
  readDistributionVersion,
  rollbackDistribution,
  runDistributionMigration,
} from '@/api/distribution'
import type {
  DistributionOverview,
  FormatReadback,
  Manifest,
  ManifestTarget,
  OverviewTarget,
} from '@/api/distribution'

const props = defineProps<{ moduleKey: string; active: boolean }>()

const targets = ref<OverviewTarget[]>([])
const manifest = ref<Manifest | null>(null)
const readback = ref<FormatReadback | null>(null)
const busy = ref<number | null>(null)
const errorMessage = ref('')
const notice = ref('')

const summary = computed(() =>
  manifest.value?.summary ?? { migrated: 0, mismatched: 0 },
)

function versionsOf(configId: number): ManifestTarget['versions'] {
  const entry = manifest.value?.targets.find((item) => item.config_id === configId)
  return entry?.versions ?? []
}

function listOrDash(items: string[]): string {
  return items.length ? items.join('、') : '—'
}

function nullSummary(counts: Record<string, number> | undefined): string {
  if (!counts) return '—'
  const entries = Object.entries(counts).filter(([, value]) => value > 0)
  if (!entries.length) return '无空值'
  return entries.map(([column, value]) => `${column}=${value}`).join('、')
}

function badgeClass(target: OverviewTarget): string {
  if (target.declared_matches_actual === false) return 'dist-badge mismatch'
  if (target.delivery_status === '已交付') return 'dist-badge ok'
  return 'dist-badge'
}

async function refresh() {
  try {
    const [overview, ledger] = await Promise.all([loadDistributionOverview(), loadManifest()])
    targets.value = overview.targets
    manifest.value = ledger
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发数据读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  notice.value = ''
  await refresh()
}

async function migrate(target: OverviewTarget, dryRun: boolean) {
  busy.value = target.config_id
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await runDistributionMigration(target.config_id, dryRun)
    if (!result.ok) {
      errorMessage.value = `${target.config_name}：${result.error || result.reason || '切换失败'}`
    } else {
      notice.value = dryRun
        ? `${target.config_name}：试跑通过，${result.verification?.row_count ?? 0} 行读回一致（未写入新版本）`
        : `${target.config_name}：已按 ${result.declared_format} 交付 v${result.version}，读回 ${result.verification?.row_count ?? 0} 行`
    }
    if (result.verification?.format === target.declared_format && result.declared_format) {
      await readBack(target, result.declared_format)
    }
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迁移执行失败'
  } finally {
    busy.value = null
  }
}

async function readBack(target: OverviewTarget, format?: string, version?: number) {
  busy.value = target.config_id
  errorMessage.value = ''
  try {
    readback.value = await readDistributionVersion(target.config_id, {
      version,
      format,
      fallback: false,
    })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '读回失败'
  } finally {
    busy.value = null
  }
}

async function rollback(target: OverviewTarget) {
  busy.value = target.config_id
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await rollbackDistribution(target.config_id)
    if (!result.ok) {
      errorMessage.value = `${target.config_name}：${result.reason || '没有可回滚的版本'}`
    } else {
      notice.value =
        `${target.config_name}：已回滚 v${result.previous_version} → v${result.current_version}，` +
        `按 ${result.readback_format} 读回 ${result.row_count} 行`
      if (result.warning) notice.value += `（${result.warning}）`
    }
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回滚失败'
  } finally {
    busy.value = null
  }
}

onMounted(() => {
  if (props.active) void refresh()
})

watch(
  () => props.active,
  (active) => {
    if (active) void refresh()
  },
)
</script>

<style scoped>
.distribution { margin-top: 24px; border-top: 1px solid var(--border); padding-top: 16px; }
.dist-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.dist-summary { font-size: 13px; color: var(--muted); align-self: center; }
.dist-notice { background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 6px; padding: 8px 10px; font-size: 13px; }
.dist-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 12px; }
.dist-card { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.dist-card-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; }
.dist-addr { display: block; color: var(--muted); font-size: 12px; }
.dist-badge { font-size: 12px; padding: 2px 8px; border-radius: 999px; background: #f1f5f9; color: #334155; white-space: nowrap; }
.dist-badge.ok { background: #dcfce7; color: #166534; }
.dist-badge.mismatch { background: #fee2e2; color: #991b1b; }
.dist-facts { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 10px 0; }
.dist-facts dt { font-size: 12px; color: var(--muted); }
.dist-facts dd { margin: 2px 0 0; font-size: 13px; }
.dist-receipt { font-size: 12px; background: #fafbfc; border: 1px solid var(--border); border-radius: 6px; padding: 8px; margin-bottom: 10px; }
.dist-receipt p { margin: 2px 0; }
.dist-label { color: var(--muted); margin-right: 6px; font-size: 12px; }
.dist-actions { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 8px; }
.dist-versions summary { cursor: pointer; font-size: 13px; color: var(--brand); }
.dist-readback { margin-top: 12px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.dist-readback-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
</style>

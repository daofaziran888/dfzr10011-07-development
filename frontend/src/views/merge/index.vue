<template>
  <section class="page" data-module="merge">
    <header class="page-head">
      <div>
        <h2>结果聚合归并管理</h2>
        <p class="page-desc">
          按分片完成水位生成归并快照：同一份快照固定一个水位，已归并数与输出数据总量同源计算，
          重复启动复用同一归并批次，失败可从缺口继续。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记聚合任务</button>
        <button class="btn" type="button" @click="exportRows">导出结果聚合归并清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>任务编号</span>
        <input v-model="filters.keyword" placeholder="按任务编号检索" />
      </label>
      <label class="filter-item">
        <span>任务状态</span>
        <select v-model="filters.status">
          <option value="">全部状态</option>
          <option v-for="status in statuses" :key="status" :value="status">{{ status }}</option>
        </select>
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <div class="control-bar">
      <label class="control-item">
        <span>生成期间有新分片完成时</span>
        <select v-model="policy">
          <option value="freeze">冻结启动时水位（新增分片计入下一批）</option>
          <option value="extend">延长到最后确认水位（下一批只保留水位之后的记录）</option>
        </select>
      </label>
      <label class="control-item">
        <span>模拟同步完成的分片</span>
        <input v-model="concurrentShards" placeholder="分片序号，如 5 或 5,6" />
      </label>
      <label class="control-item check-item">
        <input v-model="simulateWriteError" type="checkbox" />
        <span>模拟写入异常（验证整批回滚）</span>
      </label>
    </div>

    <p v-if="noticeMessage" class="notice-text">{{ noticeMessage }}</p>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ display(row, column) }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="viewLatestSnapshot(row)">查看快照</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无结果聚合归并数据，可先登记聚合任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条结果聚合归并记录</span>
      <span>快照 {{ snapshotTotal }} 份 · 刷新后按快照水位重新核对</span>
    </footer>

    <section class="panel">
      <header class="panel-head">
        <h3>归并快照</h3>
        <button class="btn ghost" type="button" @click="loadSnapshots">刷新快照</button>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>快照版本</th>
            <th>归并批次</th>
            <th>采集计划</th>
            <th>水位策略</th>
            <th>分片完成水位</th>
            <th>已归并数</th>
            <th>输出数据总量</th>
            <th>归档条目数</th>
            <th>校验</th>
            <th>生成时间</th>
            <th>归档清单</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="snapshot in snapshots" :key="String(snapshot.snapshot_id)">
            <td>{{ snapshot.版本 }}</td>
            <td>{{ snapshot.batch_id }}</td>
            <td>{{ snapshot.采集计划 }}</td>
            <td>{{ snapshot.水位策略 }}</td>
            <td>{{ snapshot.分片完成水位 }}</td>
            <td>{{ snapshot.已归并数 }}</td>
            <td>{{ snapshot.输出数据总量 }}</td>
            <td>{{ snapshot.归档条目数 }}</td>
            <td>{{ snapshot.校验通过 ? '通过' : '未通过' }}</td>
            <td>{{ snapshot.生成时间 }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="viewSnapshot(snapshot)">查看</button>
            </td>
          </tr>
          <tr v-if="!snapshots.length">
            <td colspan="11" class="empty-state">尚未生成归并快照，先对聚合任务执行「启动归并」</td>
          </tr>
        </tbody>
      </table>

      <div v-if="activeSnapshot" class="snapshot-detail">
        <div class="stat-row">
          <article class="stat-card">
            <span class="stat-label">快照水位</span>
            <strong class="stat-value">{{ activeSnapshot.分片完成水位 }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">已归并数</span>
            <strong class="stat-value">{{ activeSnapshot.已归并数 }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">输出数据总量</span>
            <strong class="stat-value">{{ activeSnapshot.输出数据总量 }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">下一批待归并数</span>
            <strong class="stat-value">{{ activeSnapshot.下一批待归并数 }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">同源核对</span>
            <strong class="stat-value">{{ consistency?.一致 ? '一致' : '待核对' }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">当前分片水位</span>
            <strong class="stat-value">{{ currentWatermark }}</strong>
          </article>
        </div>
        <p class="hint">
          启动水位 {{ activeSnapshot.启动水位 }}，最后确认水位 {{ activeSnapshot.最后确认水位 }}；
          {{ activeSnapshot.说明 }}。当前分片完成水位为 {{ currentWatermark }}，
          快照仍以生成时固定的水位 {{ activeSnapshot.分片完成水位 }} 为准。
        </p>
        <p class="hint">
          刷新或重新进入页面后按固定水位重算：已归并 {{ recompute?.重算已归并数 ?? '—' }} 片，
          输出 {{ recompute?.重算输出数据总量 ?? '—' }} 条，不再读取当前分片进度。
        </p>

        <h4 class="sub-title">归档清单（按快照版本读取）</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>归档编号</th>
              <th>分片编号</th>
              <th>分片范围</th>
              <th>记录数</th>
              <th>输出路径</th>
              <th>水位</th>
              <th>归档状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="entry in activeArchive" :key="String(entry.archive_id)">
              <td>{{ entry.archive_id }}</td>
              <td>{{ entry.分片编号 }}</td>
              <td>{{ entry.分片范围 }}</td>
              <td>{{ entry.记录数 }}</td>
              <td>{{ entry.输出路径 }}</td>
              <td>{{ entry.水位 }}</td>
              <td>{{ entry.归档状态 }}</td>
            </tr>
            <tr v-if="!activeArchive.length">
              <td colspan="7" class="empty-state">该快照水位下没有归档条目</td>
            </tr>
          </tbody>
        </table>

        <h4 class="sub-title">归并结果数据</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>分片编号</th>
              <th>分片序号</th>
              <th>记录数</th>
              <th>空分片</th>
              <th>输出路径</th>
              <th>归并结果</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in activeResults" :key="String(item.id)">
              <td>{{ item.分片编号 }}</td>
              <td>{{ item.分片序号 }}</td>
              <td>{{ item.记录数 }}</td>
              <td>{{ item.空分片 ? '是' : '否' }}</td>
              <td>{{ item.输出路径 }}</td>
              <td>{{ item.归并结果 }}</td>
            </tr>
            <tr v-if="!activeResults.length">
              <td colspan="6" class="empty-state">该快照没有归并结果</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>分片完成记录与空分片状态</h3>
        <label class="control-item">
          <span>采集计划</span>
          <select v-model="completionPlan" @change="loadCompletion">
            <option value="">全部采集计划</option>
            <option v-for="plan in plans" :key="plan.plan_id" :value="plan.plan_id">
              {{ plan.采集计划 }}（{{ plan.plan_id }}）
            </option>
          </select>
        </label>
      </header>

      <div v-for="group in completionGroups" :key="group.plan_id" class="completion-group">
        <h4 class="sub-title">
          {{ group.采集计划 }}
          <span class="tag">分片总数 {{ group.分片总数 }}</span>
          <span class="tag">完成水位 {{ group.分片完成水位 }}</span>
          <span class="tag">已完成 {{ group.已完成分片数 }}</span>
          <span class="tag">空分片 {{ group.空分片数 }}</span>
          <span v-if="group.缺口.length" class="tag warn">缺口 {{ group.缺口.join('、') }}</span>
        </h4>

        <p class="hint">分片完成记录（只统计连续完成、计入水位的分片）</p>
        <table class="data-table">
          <thead>
            <tr>
              <th>分片编号</th>
              <th>分片序号</th>
              <th>分片范围</th>
              <th>分配节点</th>
              <th>记录数</th>
              <th>完成时间</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in group.完成记录" :key="String(row.id)">
              <td>{{ row.分片编号 }}</td>
              <td>{{ row.分片序号 }}</td>
              <td>{{ row.分片范围 }}</td>
              <td>{{ row.分配节点 }}</td>
              <td>{{ row.记录数 }}</td>
              <td>{{ row.完成时间 || '—' }}</td>
            </tr>
            <tr v-if="!group.完成记录.length">
              <td colspan="6" class="empty-state">暂无分片完成记录</td>
            </tr>
          </tbody>
        </table>

        <p class="hint">空分片状态（单独展示，不产生输出数据）</p>
        <table class="data-table">
          <thead>
            <tr>
              <th>分片编号</th>
              <th>分片序号</th>
              <th>分片范围</th>
              <th>分片状态</th>
              <th>记录数</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in group.空分片" :key="`empty-${row.id}`">
              <td>{{ row.分片编号 }}</td>
              <td>{{ row.分片序号 }}</td>
              <td>{{ row.分片范围 }}</td>
              <td>{{ row.分片状态 }}</td>
              <td>{{ row.记录数 }}</td>
            </tr>
            <tr v-if="!group.空分片.length">
              <td colspan="5" class="empty-state">没有空分片</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-if="!completionGroups.length" class="empty-state">暂无采集计划的分片完成数据</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, any>
type Snapshot = Record<string, any>
type CompletionGroup = {
  plan_id: string
  采集计划: string
  分片总数: number
  分片完成水位: number
  已完成分片数: number
  空分片数: number
  缺口: number[]
  完成记录: Row[]
  空分片: Row[]
}

const ENDPOINT = '/api/merge'
const columns = [
  '任务编号',
  '采集计划',
  '分片总数',
  '分片完成水位',
  '已归并数',
  '数据总量',
  '快照版本',
  '快照已归并数',
  '快照数据总量',
  '任务状态',
]
const actions = ['启动归并', '重试归并', '归档任务']
const statuses = ['待归并', '归并中', '已完成', '归并失败']

const rows = ref<Row[]>([])
const total = ref(0)
const snapshots = ref<Snapshot[]>([])
const snapshotTotal = ref(0)
const activeSnapshot = ref<Snapshot | null>(null)
const activeArchive = ref<Row[]>([])
const activeResults = ref<Row[]>([])
const consistency = ref<Row | null>(null)
const recompute = ref<Row | null>(null)
const currentWatermark = ref<number | string>('—')
const completionGroups = ref<CompletionGroup[]>([])
const plans = ref<{ plan_id: string; 采集计划: string }[]>([])
const completionPlan = ref('')
const errorMessage = ref('')
const noticeMessage = ref('')
const filters = ref<Record<string, string>>({ keyword: '', status: '' })
const policy = ref<'freeze' | 'extend'>('freeze')
const concurrentShards = ref('')
const simulateWriteError = ref(false)

const stats = computed(() => [
  { label: '聚合任务总数', value: total.value },
  { label: '归并中任务', value: rows.value.filter((row) => row.status === '归并中').length },
  { label: '已完成任务', value: rows.value.filter((row) => row.status === '已完成').length },
  { label: '归并快照', value: snapshotTotal.value },
])

function display(row: Row, column: string): string | number {
  const value = row[column]
  return value === null || value === undefined || value === '' ? '—' : value
}

function resetFilters() {
  filters.value = { keyword: '', status: '' }
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  noticeMessage.value = '聚合任务登记入口尚未接入审批流'
}

function parseConcurrent(): (string | number)[] {
  return concurrentShards.value
    .split(/[,，\s]+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  noticeMessage.value = ''
  const values: Record<string, any> = { action, policy: policy.value }
  if (action !== '归档任务') {
    const shards = parseConcurrent()
    if (shards.length) {
      values['concurrent_shards'] = shards
    }
    if (simulateWriteError.value) {
      values['simulate_write_error'] = true
    }
  }
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      errorMessage.value = payload.message || '结果聚合归并动作未生效，请稍后重试'
      await reload()
      return
    }
    noticeMessage.value = payload.message
    if (payload.data?.snapshot) {
      await viewSnapshot(payload.data.snapshot)
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果聚合归并操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams()
  if (filters.value.keyword) query.set('keyword', filters.value.keyword)
  if (filters.value.status) query.set('status', filters.value.status)
  try {
    const response = await request(`${ENDPOINT}?${query.toString()}`)
    if (!response.ok) {
      throw new Error('聚合任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await loadSnapshots()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果聚合归并列表读取失败'
  }
}

async function loadSnapshots() {
  try {
    const response = await request(`${ENDPOINT}/snapshots`)
    if (!response.ok) throw new Error('归并快照读取失败')
    const payload = await response.json()
    snapshots.value = payload.items ?? []
    snapshotTotal.value = payload.total ?? snapshots.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归并快照读取失败'
  }
}

async function viewLatestSnapshot(row: Row) {
  if (row.快照版本 && row.快照版本 !== '—') {
    await viewSnapshot({ snapshot_version: row.快照版本 })
    return
  }
  noticeMessage.value = '该任务还没有归并快照，先执行「启动归并」'
}

async function viewSnapshot(snapshot: Row) {
  const reference = snapshot.snapshot_version ?? snapshot.snapshot_id
  try {
    const response = await request(`${ENDPOINT}/snapshots/${encodeURIComponent(String(reference))}`)
    if (!response.ok) throw new Error('归并快照明细读取失败')
    const payload = await response.json()
    activeSnapshot.value = payload.snapshot
    activeResults.value = payload.results ?? []
    activeArchive.value = payload.archive_entries ?? []
    consistency.value = payload.consistency ?? null
    recompute.value = payload.recompute ?? null
    const watermark = await request(`${ENDPOINT}/watermark?plan_id=${encodeURIComponent(payload.snapshot.plan_id)}`)
    if (watermark.ok) {
      const data = await watermark.json()
      currentWatermark.value = data.分片完成水位
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归并快照明细读取失败'
  }
}

async function loadCompletion() {
  const query = completionPlan.value ? `?plan_id=${encodeURIComponent(completionPlan.value)}` : ''
  try {
    const response = await request(`${ENDPOINT}/completion${query}`)
    if (!response.ok) throw new Error('分片完成记录读取失败')
    const payload = await response.json()
    completionGroups.value = payload.items ?? []
    if (!completionPlan.value) {
      plans.value = completionGroups.value.map((item) => ({
        plan_id: item.plan_id,
        采集计划: item.采集计划,
      }))
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分片完成记录读取失败'
  }
}

onMounted(async () => {
  await reload()
  await loadCompletion()
})
</script>

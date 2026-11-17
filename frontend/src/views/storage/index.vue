<template>
  <section class="page" data-module="storage">
    <header class="page-head">
      <div>
        <h2>采集存储分层管理</h2>
        <p class="page-desc">
          维护采集存储的分层配置、格式选择与迁移规则；新增归并闸门与分复核清单，
          存储分层按归并快照版本生成对应归档清单。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记存储策略</button>
        <button class="btn" type="button" @click="exportRows">导出采集存储分层清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <p v-if="noticeMessage" class="notice-text">{{ noticeMessage }}</p>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>归并快照版本</th>
          <th>快照水位</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.归并快照版本 ?? '—' }}</td>
          <td>{{ row.快照水位 ?? '—' }}</td>
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
            <button class="link" type="button" @click="buildManifest(row.归并快照版本)">生成归档清单</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 3" class="empty-state">暂无采集存储分层数据，可先登记存储策略</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集存储分层记录</span>
      <span>归档清单按归并快照版本读取，旧版本保留原依据</span>
    </footer>

    <section class="panel">
      <header class="panel-head">
        <h3>新增归并闸门</h3>
        <button class="btn ghost" type="button" @click="loadGates">刷新归并闸门</button>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>闸门编号</th>
            <th>采集计划</th>
            <th>归并任务</th>
            <th>归并批次</th>
            <th>快照版本</th>
            <th>分片总数</th>
            <th>分片完成水位</th>
            <th>已归并数</th>
            <th>数据总量</th>
            <th>归档条目数</th>
            <th>闸门状态</th>
            <th>更新时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="gate in gates" :key="String(gate.id)">
            <td>{{ gate.闸门编号 }}</td>
            <td>{{ gate.采集计划 }}</td>
            <td>{{ gate.归并任务 }}</td>
            <td>{{ gate.归并批次 }}</td>
            <td>{{ gate.快照版本 }}</td>
            <td>{{ gate.分片总数 }}</td>
            <td>{{ gate.分片完成水位 }}</td>
            <td>{{ gate.已归并数 }}</td>
            <td>{{ gate.数据总量 }}</td>
            <td>{{ gate.归档条目数 }}</td>
            <td>{{ gate.闸门状态 }}</td>
            <td>{{ gate.更新时间 }}</td>
          </tr>
          <tr v-if="!gates.length">
            <td colspan="12" class="empty-state">暂无归并闸门，登记聚合任务后会同步生成</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>分待办</h3>
        <button class="btn ghost" type="button" @click="loadPending">刷新分待办</button>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>待办编号</th>
            <th>采集计划</th>
            <th>任务编号</th>
            <th>快照版本</th>
            <th>当前水位</th>
            <th>下一批待归并数</th>
            <th>缺口</th>
            <th>水位策略</th>
            <th>待办状态</th>
            <th>说明</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in pending" :key="String(item.id)">
            <td>{{ item.待办编号 }}</td>
            <td>{{ item.采集计划 }}</td>
            <td>{{ item.任务编号 }}</td>
            <td>{{ item.快照版本 }}</td>
            <td>{{ item.当前水位 }}</td>
            <td>{{ item.下一批待归并数 }}</td>
            <td>{{ item.缺口 && item.缺口.length ? item.缺口.join('、') : '无' }}</td>
            <td>{{ item.水位策略 }}</td>
            <td>{{ item.待办状态 }}</td>
            <td>{{ item.说明 }}</td>
          </tr>
          <tr v-if="!pending.length">
            <td colspan="10" class="empty-state">暂无分待办</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>分复核清单</h3>
        <button class="btn ghost" type="button" @click="loadReviews">刷新复核清单</button>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>复核编号</th>
            <th>快照版本</th>
            <th>采集计划</th>
            <th>复核项</th>
            <th>校验结果</th>
            <th>复核依据</th>
            <th>复核状态</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in reviews" :key="String(item.id)">
            <td>{{ item.复核编号 }}</td>
            <td>{{ item.版本 }}</td>
            <td>{{ item.采集计划 }}</td>
            <td>{{ item.复核项 }}</td>
            <td>{{ item.校验结果 }}</td>
            <td>{{ item.复核依据 }}</td>
            <td>{{ item.复核状态 }}</td>
            <td class="row-actions">
              <button
                v-for="action in reviewActions"
                :key="action"
                class="link"
                type="button"
                @click="runReviewAction(action, item)"
              >
                {{ action }}
              </button>
            </td>
          </tr>
          <tr v-if="!reviews.length">
            <td colspan="8" class="empty-state">暂无复核项，生成归并快照后会按闸门校验逐项落地</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>归档清单</h3>
        <label class="control-item">
          <span>归并快照版本</span>
          <select v-model="manifestVersion" @change="loadManifest">
            <option value="">最新快照</option>
            <option v-for="item in snapshotVersions" :key="String(item.snapshot_id)" :value="item.snapshot_version">
              {{ item.snapshot_version }}（水位 {{ item.分片完成水位 }}）
            </option>
          </select>
        </label>
      </header>

      <div v-if="manifest" class="snapshot-summary">
        <span class="tag">版本 {{ manifest.版本 }}</span>
        <span class="tag">快照水位 {{ manifest.快照水位 }}</span>
        <span class="tag">启动水位 {{ manifest.启动水位 }}</span>
        <span class="tag">最后确认水位 {{ manifest.最后确认水位 }}</span>
        <span class="tag">{{ manifest.水位策略 }}</span>
        <span class="tag">已归并数 {{ manifest.已归并数 }}</span>
        <span class="tag">输出数据总量 {{ manifest.输出数据总量 }}</span>
        <span class="tag" :class="{ warn: manifest.归档状态 !== '完整' }">归档清单 {{ manifest.归档状态 }}</span>
      </div>
      <p v-if="manifest" class="hint">
        归档清单只包含版本 {{ manifest.版本 }} 固定的分片范围：{{ manifest.下一批待归并数 }} 个分片留待下一批。
      </p>

      <table class="data-table">
        <thead>
          <tr>
            <th>归档编号</th>
            <th>快照版本</th>
            <th>采集计划</th>
            <th>分片编号</th>
            <th>分片范围</th>
            <th>记录数</th>
            <th>输出路径</th>
            <th>水位</th>
            <th>归档状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="entry in archiveRows" :key="String(entry.archive_id)">
            <td>{{ entry.archive_id }}</td>
            <td>{{ entry.版本 }}</td>
            <td>{{ entry.采集计划 }}</td>
            <td>{{ entry.分片编号 }}</td>
            <td>{{ entry.分片范围 }}</td>
            <td>{{ entry.记录数 }}</td>
            <td>{{ entry.输出路径 }}</td>
            <td>{{ entry.水位 }}</td>
            <td>{{ entry.归档状态 }}</td>
          </tr>
          <tr v-if="!archiveRows.length">
            <td colspan="9" class="empty-state">当前版本没有归档条目</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, any>

const ENDPOINT = '/api/storage'
const columns = ['策略编号', '存储层级', '文件格式', '压缩算法', '保留天数', '迁移规则', '清理方式', '策略状态']
const actions = ['启用策略', '暂停策略', '关闭策略']
const reviewActions = ['复核通过', '标记异常', '重新打开']
const statuses = ['草稿', '已生效', '已暂停', '已关闭']
const stats = computed(() => [
  { label: '存储策略总数', value: total.value },
  { label: '已生效策略', value: rows.value.filter((row) => row.status === '已生效').length },
  { label: '已暂停策略', value: rows.value.filter((row) => row.status === '已暂停').length },
])

const rows = ref<Row[]>([])
const total = ref(0)
const gates = ref<Row[]>([])
const pending = ref<Row[]>([])
const reviews = ref<Row[]>([])
const snapshotVersions = ref<Row[]>([])
const manifest = ref<Row | null>(null)
const archiveRows = ref<Row[]>([])
const manifestVersion = ref('')
const errorMessage = ref('')
const noticeMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  noticeMessage.value = '存储策略登记入口尚未接入审批流'
}

async function getJson(path: string): Promise<Row> {
  const response = await request(path)
  if (!response.ok) {
    throw new Error(`接口返回 ${response.status}，数据未更新`)
  }
  return (await response.json()) as Row
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      errorMessage.value = payload.message || '采集存储分层动作未生效，请稍后重试'
      return
    }
    noticeMessage.value = payload.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集存储分层操作失败'
  }
}

async function runReviewAction(action: string, row: Row) {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/reviews/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      errorMessage.value = payload.message || '复核动作未生效'
      return
    }
    noticeMessage.value = payload.message
    await loadReviews()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复核动作失败'
  }
}

async function buildManifest(version: string) {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/manifest`, {
      method: 'POST',
      body: JSON.stringify({ values: { snapshot: version || '' } }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      errorMessage.value = payload.message || '归档清单生成失败'
      return
    }
    noticeMessage.value = `${payload.message}：${payload.data.版本}（水位 ${payload.data.快照水位}）`
    manifestVersion.value = payload.data.snapshot_version
    await loadManifest()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档清单生成失败'
  }
}

async function loadGates() {
  const payload = await getJson(`${ENDPOINT}/gates`)
  gates.value = payload.items ?? []
}

async function loadPending() {
  const payload = await getJson(`${ENDPOINT}/pending`)
  pending.value = payload.items ?? []
}

async function loadReviews() {
  const payload = await getJson(`${ENDPOINT}/reviews`)
  reviews.value = payload.items ?? []
}

async function loadSnapshots() {
  const payload = await getJson(`${ENDPOINT}/snapshots`)
  snapshotVersions.value = payload.items ?? []
}

async function loadManifest() {
  const query = manifestVersion.value ? `?snapshot=${encodeURIComponent(manifestVersion.value)}` : ''
  const response = await request(`${ENDPOINT}/manifest${query}`)
  if (!response.ok) {
    manifest.value = null
    manifestVersion.value = ''
    return
  }
  const payload = (await response.json()) as Row
  manifest.value = payload
  manifestVersion.value = payload.snapshot_version
  const archives = await getJson(`${ENDPOINT}/archives?snapshot=${encodeURIComponent(payload.snapshot_version)}`)
  archiveRows.value = archives.items ?? []
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const payload = await getJson(`${ENDPOINT}?${query}`)
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await Promise.all([loadGates(), loadPending(), loadReviews(), loadSnapshots()])
    await loadManifest()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集存储分层列表读取失败'
  }
}

onMounted(reload)
</script>

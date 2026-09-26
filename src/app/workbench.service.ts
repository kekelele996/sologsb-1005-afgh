import { Injectable, OnDestroy } from '@angular/core'
import { BehaviorSubject, map, type Observable } from 'rxjs'
import type { Annotation, Claim, ClaimVersion, Feature, Paragraph, Position, Role, ValidationIssue, VersionDiffEntry, WorkbenchState } from './models'

const STORAGE_KEY = 'patent-claim-mapping-workbench-v1'
const POSITION_KEY = 'patent-claim-mapping-position-v1'

const initialClaims: Claim[] = [
  { id: 'claim-1', number: 1, title: '一种自适应展柜环境控制装置', independent: true, text: '一种自适应展柜环境控制装置，包括：柜体；环境传感模块，设置于所述柜体内并用于采集温湿度数据；以及控制模块，与所述环境传感模块通信，并根据所述温湿度数据调节所述柜体的微环境。' },
  { id: 'claim-2', number: 2, title: '传感模块的布置方式', independent: false, text: '根据权利要求1所述的装置，其特征在于，所述环境传感模块包括沿所述柜体对角线布置的多个温湿度传感器。' },
  { id: 'claim-3', number: 3, title: '控制模块的调节策略', independent: false, text: '根据权利要求1所述的装置，其特征在于，所述控制模块基于历史数据与当前数据之间的偏差分级调节除湿单元。' }
]
const initialParagraphs: Paragraph[] = [
  { id: 'para-0012', section: '说明书 [0012]', text: '柜体1形成用于陈列文物的封闭空间。环境传感模块2安装于柜体内部，可采集温度、相对湿度等环境数据，并将数据发送至控制模块3。' },
  { id: 'para-0018', section: '说明书 [0018]', text: '在一种实施方式中，多个温湿度传感器沿柜体对角线布置，由此可降低局部气流造成的测量偏差。传感器数量可根据柜体容积设定。' },
  { id: 'para-0024', section: '说明书 [0024]', text: '控制模块可比较当前湿度与预设区间，并结合历史变化趋势生成调节等级。当偏差持续超过阈值时，控制模块启动除湿单元并提高调节频率。' },
  { id: 'para-0031', section: '说明书 [0031]', text: '控制模块与传感模块之间可以采用有线或无线通信。通信链路可周期传输数据，传输周期例如为十秒至五分钟。' },
  { id: 'para-0040', section: '说明书 [0040]', text: '微环境调节包括湿度调节、温度调节及气体交换。控制策略可记录执行结果，用于后续趋势判断。' }
]
const initialFeatures: Feature[] = [
  { id: 'feature-a', claimId: 'claim-1', label: 'A · 柜体', text: '柜体', parentId: null, referenceIds: [], supportIds: ['para-0012'], ownerRole: 'author' },
  { id: 'feature-b', claimId: 'claim-1', label: 'B · 环境传感模块', text: '设置于柜体内，用于采集温湿度数据', parentId: 'feature-a', referenceIds: [], supportIds: ['para-0012', 'para-0018'], ownerRole: 'author' },
  { id: 'feature-c', claimId: 'claim-1', label: 'C · 控制模块通信', text: '与环境传感模块通信', parentId: 'feature-a', referenceIds: ['feature-b'], supportIds: ['para-0012', 'para-0031'], ownerRole: 'author' },
  { id: 'feature-d', claimId: 'claim-1', label: 'D · 调节微环境', text: '根据温湿度数据调节柜体微环境', parentId: null, referenceIds: ['feature-b', 'feature-c'], supportIds: ['para-0024', 'para-0040'], ownerRole: 'author' },
  { id: 'feature-e', claimId: 'claim-2', label: 'E · 对角线布置', text: '多个温湿度传感器沿柜体对角线布置', parentId: null, referenceIds: [], supportIds: ['para-0018'], ownerRole: 'author' },
  { id: 'feature-f', claimId: 'claim-3', label: 'F · 分级调节', text: '基于历史数据与当前数据的偏差分级调节除湿单元', parentId: null, referenceIds: [], supportIds: ['para-0024'], ownerRole: 'author' }
]
const initialAnnotations: Annotation[] = [
  { id: 'annotation-1', featureId: 'feature-b', authorRole: 'examiner', authorName: '审查员 · 李岚', text: '“温湿度数据”是否包括露点等派生数据？建议在从属权利要求中限定。', updatedAt: '2026-09-24T03:10:00.000Z' },
  { id: 'annotation-2', featureId: 'feature-d', authorRole: 'author', authorName: '代理人 · 陈昊', text: '[0024] 已支持分级调节，发布前补充除湿单元与通信模块的连接关系。', updatedAt: '2026-09-24T04:05:00.000Z' }
]
function demoState(): WorkbenchState {
  return {
    claims: initialClaims, paragraphs: initialParagraphs, features: initialFeatures,
    annotations: initialAnnotations, orphanMappings: [], versions: [],
    role: 'author', currentUserRole: 'author', selectedClaimId: 'claim-1', selectedFeatureId: 'feature-b', activeTab: 'mapping'
  }
}
function clone<T>(value: T): T { return structuredClone(value) }

@Injectable({ providedIn: 'root' })
export class WorkbenchService implements OnDestroy {
  private readonly initialState = this.loadState()
  private readonly stateSubject = new BehaviorSubject<WorkbenchState>(this.initialState)
  private readonly historySubject = new BehaviorSubject<{ past: number; future: number }>({ past: 0, future: 0 })
  private past: WorkbenchState[] = []
  private future: WorkbenchState[] = []

  readonly state$ = this.stateSubject.asObservable()
  readonly history$ = this.historySubject.asObservable()
  readonly claims$ = this.state$.pipe(map(state => state.claims))
  readonly paragraphs$ = this.state$.pipe(map(state => state.paragraphs))
  readonly features$ = this.state$.pipe(map(state => state.features))
  readonly annotations$ = this.state$.pipe(map(state => state.annotations))
  readonly role$ = this.state$.pipe(map(state => state.role))
  readonly selectedClaim$ = this.state$.pipe(map(state => state.claims.find(claim => claim.id === state.selectedClaimId) || state.claims[0]))
  readonly selectedFeature$ = this.state$.pipe(map(state => state.features.find(feature => feature.id === state.selectedFeatureId) || null))
  readonly issues$ = this.state$.pipe(map(state => this.validate(state)))

  constructor() {
    if (typeof window !== 'undefined') window.addEventListener('beforeunload', () => this.savePosition())
  }

  ngOnDestroy(): void {
    if (typeof window !== 'undefined') window.removeEventListener('beforeunload', () => this.savePosition())
  }

  get snapshot(): WorkbenchState { return clone(this.stateSubject.value) }
  get canUndo(): boolean { return this.past.length > 0 }
  get canRedo(): boolean { return this.future.length > 0 }

  selectClaim(id: string): void {
    this.patchState(state => { state.selectedClaimId = id; state.selectedFeatureId = state.features.find(feature => feature.claimId === id)?.id || null })
    this.savePosition()
  }

  selectFeature(id: string | null): void {
    this.patchState(state => { state.selectedFeatureId = id })
    this.savePosition()
  }

  setRole(role: Role): void {
    this.patchState(state => { state.role = role; state.currentUserRole = role })
  }

  setTab(tab: string): void {
    this.patchState(state => { state.activeTab = tab })
    this.savePosition()
  }

  updateClaim(patch: Partial<Claim>): void {
    this.commit(state => {
      const claim = state.claims.find(item => item.id === state.selectedClaimId)
      if (claim) Object.assign(claim, patch)
    })
  }

  addClaim(): void {
    this.commit(state => {
      const number = Math.max(0, ...state.claims.map(claim => claim.number)) + 1
      const claim: Claim = { id: `claim-${Date.now()}`, number, title: `权利要求 ${number}`, independent: false, text: '请录入权利要求正文。' }
      state.claims.push(claim)
      state.selectedClaimId = claim.id
      state.selectedFeatureId = null
    })
  }

  addParagraph(): void {
    if (this.stateSubject.value.role === 'viewer') return
    this.commit(state => {
      const next = state.paragraphs.length + 1
      state.paragraphs.push({ id: `para-${Date.now()}`, section: `说明书 [${String(next * 5).padStart(4, '0')}]`, text: '' })
    })
  }

  updateParagraph(id: string, patch: Partial<Paragraph>): void {
    if (this.stateSubject.value.role === 'viewer') return
    this.commit(state => {
      const paragraph = state.paragraphs.find(item => item.id === id)
      if (paragraph) Object.assign(paragraph, patch)
    })
  }

  deleteParagraph(id: string): void {
    if (this.stateSubject.value.role === 'viewer') return
    this.commit(state => {
      state.paragraphs = state.paragraphs.filter(item => item.id !== id)
      state.features.forEach(feature => { feature.supportIds = feature.supportIds.filter(paragraphId => paragraphId !== id) })
      state.orphanMappings = state.orphanMappings.filter(item => item.paragraphId !== id)
    })
  }

  addFeature(): void {
    if (this.stateSubject.value.role === 'viewer') return
    this.commit(state => {
      const feature: Feature = {
        id: `feature-${Date.now()}`, claimId: state.selectedClaimId,
        label: `新特征 ${state.features.filter(item => item.claimId === state.selectedClaimId).length + 1}`,
        text: '', parentId: null, referenceIds: [], supportIds: [], ownerRole: state.role
      }
      state.features.push(feature)
      state.selectedFeatureId = feature.id
    })
  }

  updateFeature(id: string, patch: Partial<Feature>): void {
    if (this.stateSubject.value.role === 'viewer') return
    this.commit(state => {
      const feature = state.features.find(item => item.id === id)
      if (feature) Object.assign(feature, patch)
    })
  }

  deleteFeature(id: string): void {
    if (this.stateSubject.value.role === 'viewer') return
    this.commit(state => {
      const feature = state.features.find(item => item.id === id)
      if (!feature) return
      feature.supportIds.forEach(paragraphId => state.orphanMappings.push({
        id: `orphan-${Date.now()}-${paragraphId}`, featureLabel: feature.label, paragraphId,
        reason: `技术特征“${feature.label}”已删除，但支持段落映射仍被保留。`
      }))
      state.features = state.features.filter(item => item.id !== id)
      state.features.forEach(item => {
        item.referenceIds = item.referenceIds.filter(refId => refId !== id)
        if (item.parentId === id) item.parentId = null
      })
      state.annotations = state.annotations.filter(item => item.featureId !== id)
      state.selectedFeatureId = state.features.find(item => item.claimId === state.selectedClaimId)?.id || null
    })
  }

  toggleParagraphMapping(featureId: string, paragraphId: string): void {
    if (this.stateSubject.value.role === 'viewer') return
    this.commit(state => {
      const feature = state.features.find(item => item.id === featureId)
      if (!feature) return
      const index = feature.supportIds.indexOf(paragraphId)
      if (index >= 0) feature.supportIds.splice(index, 1)
      else feature.supportIds.push(paragraphId)
      state.orphanMappings = state.orphanMappings.filter(item => item.paragraphId !== paragraphId)
    })
  }

  clearOrphan(id: string): void {
    this.commit(state => { state.orphanMappings = state.orphanMappings.filter(item => item.id !== id) })
  }

  addAnnotation(featureId: string, text: string): void {
    const trimmed = text.trim()
    if (!trimmed) return
    const role = this.stateSubject.value.role
    const names: Record<Role, string> = { author: '代理人 · 陈昊', examiner: '审查员 · 李岚', viewer: '观察者' }
    this.commit(state => state.annotations.push({
      id: `annotation-${Date.now()}`, featureId, authorRole: role, authorName: names[role], text: trimmed, updatedAt: new Date().toISOString()
    }))
  }

  updateAnnotation(id: string, text: string): void {
    this.commit(state => {
      const annotation = state.annotations.find(item => item.id === id)
      if (annotation && annotation.authorRole === state.role) annotation.text = text
    })
  }

  deleteAnnotation(id: string): void {
    this.commit(state => {
      const annotation = state.annotations.find(item => item.id === id)
      if (annotation && annotation.authorRole === state.role) state.annotations = state.annotations.filter(item => item.id !== id)
    })
  }

  createVersion(name?: string): void {
    this.commit(state => {
      state.versions.unshift({
        id: `version-${Date.now()}`, name: name?.trim() || `快照 ${new Date().toLocaleString('zh-CN', { hour12: false })}`,
        createdAt: new Date().toISOString(),
        claims: clone(state.claims), features: clone(state.features), paragraphs: clone(state.paragraphs),
        annotations: clone(state.annotations), orphanMappings: clone(state.orphanMappings)
      })
    })
  }

  restoreVersion(id: string): void {
    this.commit(state => {
      const version = state.versions.find(item => item.id === id)
      if (!version) return
      state.claims = clone(version.claims)
      state.features = clone(version.features)
      state.paragraphs = clone(version.paragraphs || [])
      state.annotations = clone(version.annotations || [])
      state.orphanMappings = clone(version.orphanMappings || [])
      if (!state.claims.some(claim => claim.id === state.selectedClaimId)) state.selectedClaimId = state.claims[0]?.id || ''
      state.selectedFeatureId = state.features.find(feature => feature.claimId === state.selectedClaimId)?.id || null
    })
  }

  compareVersions(aId: string, bId: string): VersionDiffEntry[] {
    const versions = this.stateSubject.value.versions
    const a = versions.find(item => item.id === aId)
    const b = versions.find(item => item.id === bId)
    if (!a || !b) return []
    const entries: VersionDiffEntry[] = []
    let sequence = 0
    const push = (entry: Omit<VersionDiffEntry, 'id'>): void => { entries.push({ id: `diff-${sequence++}`, ...entry }) }
    const paragraphsOf = (version: ClaimVersion): Paragraph[] => version.paragraphs || []
    const annotationsOf = (version: ClaimVersion): Annotation[] => version.annotations || []
    const featureLabelIn = (version: ClaimVersion, id: string | null): string => {
      if (!id) return '顶层特征'
      return version.features.find(item => item.id === id)?.label || '已删除特征'
    }
    const paragraphSectionIn = (version: ClaimVersion, id: string): string =>
      paragraphsOf(version).find(item => item.id === id)?.section || id
    const claimNumberOf = (version: ClaimVersion, claimId: string): number | null =>
      version.claims.find(item => item.id === claimId)?.number ?? null

    const claimIds = Array.from(new Set([...a.claims.map(item => item.id), ...b.claims.map(item => item.id)]))
      .sort((x, y) => (claimNumberOf(b, x) ?? claimNumberOf(a, x) ?? 0) - (claimNumberOf(b, y) ?? claimNumberOf(a, y) ?? 0))
    for (const id of claimIds) {
      const before = a.claims.find(item => item.id === id)
      const after = b.claims.find(item => item.id === id)
      const label = `权利要求 ${after?.number ?? before?.number ?? '?'}`
      if (!before && after) {
        push({ scope: 'claim', category: 'added', label, detail: `目标版本新增${label}《${after.title}》。`, before: '', after: after.text })
      } else if (before && !after) {
        push({ scope: 'claim', category: 'removed', label, detail: `目标版本删除了${label}《${before.title}》。`, before: before.text, after: '' })
      } else if (before && after) {
        if (before.text !== after.text) push({ scope: 'claim', category: 'text', label, detail: `${label}正文有修改。`, before: before.text, after: after.text })
        if (before.title !== after.title) push({ scope: 'claim', category: 'text', label, detail: `名称由「${before.title}」改为「${after.title}」。`, before: before.title, after: after.title })
        if (before.number !== after.number) push({ scope: 'claim', category: 'structure', label, detail: `编号由第 ${before.number} 项调整为第 ${after.number} 项。`, before: `第 ${before.number} 项`, after: `第 ${after.number} 项` })
        if (before.independent !== after.independent) push({ scope: 'claim', category: 'structure', label, detail: `由${before.independent ? '独立' : '从属'}权利要求改为${after.independent ? '独立' : '从属'}权利要求。`, before: before.independent ? '独立权利要求' : '从属权利要求', after: after.independent ? '独立权利要求' : '从属权利要求' })
      }
    }

    const featureIds = Array.from(new Set([...a.features.map(item => item.id), ...b.features.map(item => item.id)]))
      .sort((x, y) => {
        const fx = b.features.find(item => item.id === x) || a.features.find(item => item.id === x)
        const fy = b.features.find(item => item.id === y) || a.features.find(item => item.id === y)
        const nx = fx ? (claimNumberOf(b, fx.claimId) ?? claimNumberOf(a, fx.claimId) ?? 0) : 0
        const ny = fy ? (claimNumberOf(b, fy.claimId) ?? claimNumberOf(a, fy.claimId) ?? 0) : 0
        return nx - ny || (fx?.label || '').localeCompare(fy?.label || '', 'zh-CN')
      })
    for (const id of featureIds) {
      const before = a.features.find(item => item.id === id)
      const after = b.features.find(item => item.id === id)
      const claimNumber = (after && (claimNumberOf(b, after.claimId) ?? claimNumberOf(a, after.claimId)))
        ?? (before && (claimNumberOf(a, before.claimId) ?? claimNumberOf(b, before.claimId))) ?? '?'
      const label = `权利要求 ${claimNumber} · ${(after || before)?.label || '未命名特征'}`
      if (!before && after) {
        push({ scope: 'feature', category: 'added', label, detail: `目标版本新增特征「${after.label}」。`, before: '', after: after.text })
      } else if (before && !after) {
        push({ scope: 'feature', category: 'removed', label, detail: `目标版本删除了特征「${before.label}」。`, before: before.text, after: '' })
      } else if (before && after) {
        if (before.label !== after.label) push({ scope: 'feature', category: 'text', label, detail: `特征名称由「${before.label}」改为「${after.label}」。`, before: before.label, after: after.label })
        if (before.text !== after.text) push({ scope: 'feature', category: 'text', label, detail: '特征正文有修改。', before: before.text, after: after.text })
        if (before.claimId !== after.claimId) push({ scope: 'feature', category: 'structure', label, detail: `所属权利要求由第 ${claimNumberOf(a, before.claimId) ?? '?'} 项移至第 ${claimNumberOf(b, after.claimId) ?? '?'} 项。`, before: `权利要求 ${claimNumberOf(a, before.claimId) ?? '?'}`, after: `权利要求 ${claimNumberOf(b, after.claimId) ?? '?'}` })
        if (before.parentId !== after.parentId) push({ scope: 'feature', category: 'structure', label, detail: `特征层级变化：父级由「${featureLabelIn(a, before.parentId)}」变为「${featureLabelIn(b, after.parentId)}」。`, before: featureLabelIn(a, before.parentId), after: featureLabelIn(b, after.parentId) })
        const refsAdded = after.referenceIds.filter(refId => !before.referenceIds.includes(refId))
        const refsRemoved = before.referenceIds.filter(refId => !after.referenceIds.includes(refId))
        if (refsAdded.length || refsRemoved.length) push({
          scope: 'feature', category: 'structure', label,
          detail: `引用关系变化：${[refsAdded.length ? `新增引用 ${refsAdded.map(refId => `「${featureLabelIn(b, refId)}」`).join('、')}` : '', refsRemoved.length ? `移除引用 ${refsRemoved.map(refId => `「${featureLabelIn(a, refId)}」`).join('、')}` : ''].filter(Boolean).join('；')}。`,
          before: before.referenceIds.map(refId => featureLabelIn(a, refId)).join('；') || '（无引用）',
          after: after.referenceIds.map(refId => featureLabelIn(b, refId)).join('；') || '（无引用）'
        })
        const supportsAdded = after.supportIds.filter(paragraphId => !before.supportIds.includes(paragraphId))
        const supportsRemoved = before.supportIds.filter(paragraphId => !after.supportIds.includes(paragraphId))
        if (supportsAdded.length || supportsRemoved.length) push({
          scope: 'feature', category: 'structure', label,
          detail: `支持映射变化：${[supportsAdded.length ? `新增 ${supportsAdded.map(paragraphId => paragraphSectionIn(b, paragraphId)).join('、')}` : '', supportsRemoved.length ? `移除 ${supportsRemoved.map(paragraphId => paragraphSectionIn(a, paragraphId)).join('、')}` : ''].filter(Boolean).join('；')}。`,
          before: before.supportIds.map(paragraphId => paragraphSectionIn(a, paragraphId)).join('；') || '（无映射）',
          after: after.supportIds.map(paragraphId => paragraphSectionIn(b, paragraphId)).join('；') || '（无映射）'
        })
      }
      const annotationIds = Array.from(new Set([
        ...annotationsOf(a).filter(item => item.featureId === id).map(item => item.id),
        ...annotationsOf(b).filter(item => item.featureId === id).map(item => item.id)
      ]))
      for (const annotationId of annotationIds) {
        const beforeAnnotation = annotationsOf(a).find(item => item.id === annotationId)
        const afterAnnotation = annotationsOf(b).find(item => item.id === annotationId)
        if (!beforeAnnotation && afterAnnotation) {
          push({ scope: 'feature', category: 'annotation', label, detail: `新增批注（${afterAnnotation.authorName}）。`, before: '', after: afterAnnotation.text })
        } else if (beforeAnnotation && !afterAnnotation) {
          push({ scope: 'feature', category: 'annotation', label, detail: `删除了 ${beforeAnnotation.authorName} 的批注。`, before: beforeAnnotation.text, after: '' })
        } else if (beforeAnnotation && afterAnnotation && beforeAnnotation.text !== afterAnnotation.text) {
          push({ scope: 'feature', category: 'annotation', label, detail: `${afterAnnotation.authorName} 的批注内容有修改。`, before: beforeAnnotation.text, after: afterAnnotation.text })
        }
      }
    }

    const paragraphIds = Array.from(new Set([...paragraphsOf(a).map(item => item.id), ...paragraphsOf(b).map(item => item.id)]))
      .sort((x, y) => {
        const sx = paragraphsOf(b).find(item => item.id === x)?.section || paragraphsOf(a).find(item => item.id === x)?.section || ''
        const sy = paragraphsOf(b).find(item => item.id === y)?.section || paragraphsOf(a).find(item => item.id === y)?.section || ''
        return sx.localeCompare(sy, 'zh-CN')
      })
    for (const id of paragraphIds) {
      const before = paragraphsOf(a).find(item => item.id === id)
      const after = paragraphsOf(b).find(item => item.id === id)
      const label = after?.section || before?.section || id
      if (!before && after) {
        push({ scope: 'paragraph', category: 'added', label, detail: `目标版本新增说明书段落 ${after.section}。`, before: '', after: after.text })
      } else if (before && !after) {
        push({ scope: 'paragraph', category: 'removed', label, detail: `目标版本删除了说明书段落 ${before.section}。`, before: before.text, after: '' })
      } else if (before && after) {
        if (before.section !== after.section) push({ scope: 'paragraph', category: 'basis', label, detail: `段落编号由「${before.section}」改为「${after.section}」。`, before: before.section, after: after.section })
        if (before.text !== after.text) push({ scope: 'paragraph', category: 'basis', label, detail: '说明书依据文字有修改。', before: before.text, after: after.text })
      }
    }
    return entries
  }

  undo(): void {
    const previous = this.past.pop()
    if (!previous) return
    this.future.push(clone(this.stateSubject.value))
    this.stateSubject.next(previous)
    this.updateHistory()
    this.saveState()
  }

  redo(): void {
    const next = this.future.pop()
    if (!next) return
    this.past.push(clone(this.stateSubject.value))
    this.stateSubject.next(next)
    this.updateHistory()
    this.saveState()
  }

  savePosition(): void {
    if (typeof localStorage === 'undefined') return
    const state = this.stateSubject.value
    const position: Position = { tab: state.activeTab, claimId: state.selectedClaimId, featureId: state.selectedFeatureId, scrollY: window.scrollY }
    localStorage.setItem(POSITION_KEY, JSON.stringify(position))
    this.saveState()
  }

  readPosition(): Position {
    if (typeof localStorage === 'undefined') return { tab: this.initialState.activeTab, claimId: this.initialState.selectedClaimId, featureId: this.initialState.selectedFeatureId, scrollY: 0 }
    try { return { ...JSON.parse(localStorage.getItem(POSITION_KEY) || '{}'), ...this.stateSubject.value } } catch { return { tab: 'mapping', claimId: this.initialState.selectedClaimId, featureId: this.initialState.selectedFeatureId, scrollY: 0 } }
  }

  exportJson(): string { return JSON.stringify({ ...this.snapshot, validationIssues: this.validate(this.stateSubject.value) }, null, 2) }

  exportCsv(): string {
    const state = this.stateSubject.value
    const rows = state.features.map(feature => [
      state.claims.find(claim => claim.id === feature.claimId)?.number || '', feature.label, feature.text,
      state.features.find(item => item.id === feature.parentId)?.label || '',
      feature.referenceIds.map(id => state.features.find(item => item.id === id)?.label || id).join('；'),
      feature.supportIds.map(id => state.paragraphs.find(item => item.id === id)?.section || id).join('；')
    ])
    const csv = [['权利要求', '技术特征', '特征内容', '父级特征', '引用特征', '支持段落'], ...rows]
      .map(row => row.map(value => `"${String(value).replaceAll('"', '""')}"`).join(',')).join('\n')
    return `\uFEFF${csv}`
  }

  validate(state = this.stateSubject.value): ValidationIssue[] {
    const issues: ValidationIssue[] = []
    for (const feature of state.features) {
      if (!feature.text.trim()) issues.push({ id: `empty-${feature.id}`, severity: 'warning', type: 'empty-feature', featureId: feature.id, title: `${feature.label} 内容为空`, detail: '请补全技术特征文字，避免映射对象不明确。' })
      if (!feature.supportIds.length) issues.push({ id: `support-${feature.id}`, severity: 'error', type: 'missing-support', featureId: feature.id, title: `${feature.label} 缺少说明书依据`, detail: '至少为一个说明书段落建立支持映射。' })
      if (this.hasReferenceCycle(feature, state.features)) issues.push({ id: `cycle-${feature.id}`, severity: 'error', type: 'cycle', featureId: feature.id, title: `${feature.label} 存在循环引用`, detail: '特征层级或引用关系形成闭环，请移除其中一条关系。' })
    }
    state.orphanMappings.forEach(item => issues.push({ id: item.id, severity: 'warning', type: 'orphan-mapping', title: '存在待清理映射', detail: item.reason }))
    return issues
  }

  private hasReferenceCycle(start: Feature, features: Feature[]): boolean {
    const visited = new Set<string>()
    const visit = (id: string): boolean => {
      if (id === start.id && visited.size > 0) return true
      if (visited.has(id)) return false
      visited.add(id)
      const feature = features.find(item => item.id === id)
      if (!feature) return false
      if (feature.parentId && visit(feature.parentId)) return true
      return feature.referenceIds.some(visit)
    }
    return visit(start.id)
  }

  private commit(recipe: (state: WorkbenchState) => void): void {
    const current = clone(this.stateSubject.value)
    const next = clone(current)
    recipe(next)
    this.past.push(current)
    if (this.past.length > 60) this.past.shift()
    this.future = []
    this.stateSubject.next(next)
    this.updateHistory()
    this.saveState()
  }

  private patchState(recipe: (state: WorkbenchState) => void): void {
    const next = clone(this.stateSubject.value)
    recipe(next)
    this.stateSubject.next(next)
    this.saveState()
  }

  private updateHistory(): void { this.historySubject.next({ past: this.past.length, future: this.future.length }) }
  private saveState(): void { if (typeof localStorage !== 'undefined') localStorage.setItem(STORAGE_KEY, JSON.stringify(this.stateSubject.value)) }
  private loadState(): WorkbenchState {
    if (typeof localStorage === 'undefined') return demoState()
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      return stored ? { ...demoState(), ...JSON.parse(stored) } : demoState()
    } catch { return demoState() }
  }
}

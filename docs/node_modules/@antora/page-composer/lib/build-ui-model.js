'use strict'

const { posix: path } = require('path')
const { URL } = require('url')

const { DEFAULT_LAYOUT_NAME } = require('./constants')
const { version: VERSION } = require('../package.json')

function buildBaseUiModel (playbook, contentCatalog, env) {
  const contentCatalogModel = contentCatalog.exportToModel()
  return {
    antoraVersion: VERSION,
    contentCatalog: contentCatalogModel,
    env,
    site: buildSiteUiModel(playbook, contentCatalogModel),
  }
}

function buildUiModel (baseUiModel, file, contentCatalog, navigationCatalog) {
  const siteUiModel = baseUiModel.site
  const siteRootPath = file.pub.rootPath || siteUiModel.path || ''
  const uiRootPath = siteRootPath + siteUiModel.ui.url
  return Object.assign({}, baseUiModel, {
    page: buildPageUiModel(siteUiModel, file, contentCatalog, navigationCatalog),
    siteRootPath,
    uiRootPath,
  })
}

function buildSiteUiModel (playbook, contentCatalog) {
  const model = { title: playbook.site.title }

  let siteUrl = playbook.site.url
  if (siteUrl) {
    if (siteUrl === '/') {
      model.url = siteUrl
      model.path = ''
    } else {
      if (siteUrl.charAt(siteUrl.length - 1) === '/') siteUrl = siteUrl.substr(0, siteUrl.length - 1)
      if (siteUrl.charAt() === '/') {
        model.path = siteUrl
      } else if ((model.path = new URL(siteUrl).pathname) === '/') {
        model.path = ''
      }
      model.url = siteUrl
    }
  }

  const startPage = contentCatalog.getSiteStartPage()
  if (startPage) model.homeUrl = startPage.pub.url

  model.components = contentCatalog.getComponentsSortedBy('title').reduce((map, it) => (map[it.name] = it) && map, {})
  model.keys = Object.assign({}, playbook.site.keys)

  const uiConfig = playbook.ui
  model.ui = {
    url: path.resolve('/', uiConfig.outputDir),
    defaultLayout: uiConfig.defaultLayout || DEFAULT_LAYOUT_NAME,
  }

  return model
}

function buildPageUiModel (siteUiModel, file, contentCatalog, navigationCatalog) {
  const src = file.src
  if (src.stem === '404' && !src.component) return { layout: '404', title: file.title }
  const { component: component_, version, module: module_, relative: relativeSrcPath, origin, editUrl, fileUri } = src

  // QUESTION should attributes be scoped to AsciiDoc, or should this work regardless of markup language? file.data?
  const asciidoc = file.asciidoc || {}
  const attributes = asciidoc.attributes || {}
  const pageAttributes = Object.entries(attributes).reduce((accum, [name, val]) => {
    if (name.startsWith('page-')) accum[name.substr(5)] = val
    return accum
  }, {})

  const url = file.pub.url
  const component = contentCatalog.getComponent(component_)
  const componentVersion = contentCatalog.getComponentVersion(component, version)
  // QUESTION can we cache versions on file.rel so only computed once per page version lineage?
  const versions = component.versions.length > 1 ? getPageVersions(src, component, contentCatalog) : undefined
  const title = file.title || asciidoc.doctitle

  const model = {
    contents: file.contents,
    layout: pageAttributes.layout || siteUiModel.ui.defaultLayout,
    title,
    url,
    description: attributes.description,
    keywords: attributes.keywords,
    role: attributes.docrole,
    attributes: pageAttributes,
    component,
    version,
    displayVersion: componentVersion.displayVersion,
    componentVersion,
    module: module_,
    relativeSrcPath,
    origin,
    versions,
    editUrl,
    fileUri,
    home: url === siteUiModel.homeUrl,
  }

  if (navigationCatalog) attachNavProperties(model, url, title, navigationCatalog.getNavigation(component_, version))

  if (versions) {
    Object.defineProperty(model, 'latest', {
      get () {
        return this.versions.find((candidate) => candidate.latest)
      },
    })
  }

  // NOTE site URL has already been normalized at this point
  const siteUrl = siteUiModel.url
  if (siteUrl && siteUrl.charAt() !== '/') {
    if (versions) {
      let latestReached
      // NOTE latest not guaranteed to match latest component version since the page may be missing in that version
      const latest = versions.find((it) => (latestReached || (latestReached = it.latest)) && !it.missing)
      // NOTE latest can be undefined if page is only in prerelease and the prerelease version is not the latest version
      if (latest && !latest.prerelease) {
        let canonicalUrl = latest.url
        if (canonicalUrl === url || canonicalUrl.charAt() === '/') canonicalUrl = siteUrl + canonicalUrl
        model.canonicalUrl = file.pub.canonicalUrl = canonicalUrl
      }
    } else if (!componentVersion.prerelease) {
      model.canonicalUrl = file.pub.canonicalUrl = siteUrl + url
    }
  }

  return model
}

// QUESTION should this function go in ContentCatalog?
// QUESTION should this function accept component, module, relative instead of pageSrc?
function getPageVersions (pageSrc, component, contentCatalog) {
  const basePageId = { component: component.name, module: pageSrc.module, family: 'page', relative: pageSrc.relative }
  return component.versions.map((componentVersion) => {
    const page = contentCatalog.getById(Object.assign({ version: componentVersion.version }, basePageId))
    // QUESTION should title be title of component or page?
    return Object.assign(
      componentVersion === component.latest ? { latest: true } : {},
      componentVersion,
      page ? { url: page.pub.url } : { missing: true }
    )
  })
}

function attachNavProperties (model, currentUrl, title, navigation = []) {
  if (!(model.navigation = navigation).length) return
  const startPageUrl = model.componentVersion.url
  const { match, ancestors, previous, next } = findNavItem({ ancestors: [], seekNext: true, currentUrl }, navigation)
  if (match) {
    // QUESTION should we filter out component start page from the breadcrumbs?
    const breadcrumbs = ancestors.filter((item) => 'content' in item)
    const parent = breadcrumbs.find((item) => item.urlType === 'internal')
    breadcrumbs.reverse().push(match)
    model.breadcrumbs = breadcrumbs
    if (parent) model.parent = parent
    if (previous) {
      model.previous = previous
    } else if (currentUrl !== startPageUrl) {
      model.previous = { content: model.componentVersion.title, url: startPageUrl, urlType: 'internal', discrete: true }
    }
    if (next) model.next = next
  } else {
    const orphan = { content: title, url: currentUrl, urlType: 'internal', discrete: true }
    if (title) model.breadcrumbs = [orphan]
    if (currentUrl === startPageUrl) {
      const { next: first } = findNavItem({ ancestors: [], match: orphan, seekNext: true, currentUrl }, navigation)
      if (first) model.next = first
    }
  }
}

function findNavItem (correlated, siblings, root = true, siblingIdx = 0, candidate = undefined) {
  if (!(candidate = candidate || siblings[siblingIdx])) {
    return correlated
  } else if (correlated.match) {
    if (candidate.urlType === 'internal' && !matchesPage(candidate, correlated.currentUrl)) {
      correlated.next = candidate
      return correlated
    }
  } else if (candidate.urlType === 'internal') {
    if (matchesPage(candidate, correlated.currentUrl)) {
      correlated.match = candidate
      if (!correlated.seekNext) return correlated
    } else if (!(correlated.previous && matchesPage(candidate, correlated.previous.url, correlated.previous.hash))) {
      correlated.previous = candidate
    }
  }
  const children = candidate.items || []
  if (children.length) {
    const ancestors = correlated.ancestors
    correlated = findNavItem(
      correlated.match ? correlated : Object.assign({}, correlated, { ancestors: [candidate].concat(ancestors) }),
      children,
      false
    )
    if (correlated.match) {
      if (!correlated.seekNext || correlated.next) return correlated
    } else {
      correlated.ancestors = ancestors
    }
  }
  if (++siblingIdx < siblings.length) {
    correlated = findNavItem(correlated, siblings, root, siblingIdx)
  } else if (root && !correlated.match) {
    delete correlated.previous
  }
  return correlated
}

function matchesPage (candidate, url, hash = undefined) {
  return candidate.url === (hash ? url.substr(0, url.length - hash.length) : url) + (candidate.hash || '')
}

module.exports = { buildBaseUiModel, buildSiteUiModel, buildPageUiModel, buildUiModel }

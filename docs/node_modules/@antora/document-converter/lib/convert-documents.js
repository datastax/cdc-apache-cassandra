'use strict'

const convertDocument = require('./convert-document')
const { loadAsciiDoc, extractAsciiDocMetadata } = require('@antora/asciidoc-loader')

/**
 * Converts the contents of publishable pages with the media type text/asciidoc
 * in the content catalog to embedded HTML.
 *
 * First finds all publishable AsciiDoc files (identified by the media type
 * text/asciidoc) in the page family in the content catalog. Then runs through
 * all those files and loads the document header into the asciidoc property on
 * the file and registers any page aliases. If the page-partial attribute is
 * set, it backs up the AsciiDoc source to the src.contents property. It then
 * runs through all the pages again and converts the contents to embedded HTML
 * by delegating to the convertDocument function. The function then returns all
 * the files in the page family (even those which don't have the media type
 * text/asciidoc). All the files returned from this function are intended to be
 * composed (i.e., wrapped in an HTML layout) by the page composer.
 *
 * @memberof document-converter
 *
 * @param {ContentCatalog} contentCatalog - The catalog of all virtual content files in the site.
 * @param {Object} [siteAsciiDocConfig={}] - Site-wide AsciiDoc processor configuration options.
 *
 * @returns {Array<File>} The publishable virtual files in the page family taken from the content catalog.
 */
function convertDocuments (contentCatalog, siteAsciiDocConfig = {}) {
  const mainAsciiDocConfigs = new Map()
  contentCatalog.getComponents().forEach(({ name: component, versions }) => {
    versions.forEach(({ version, asciidoc }) => {
      mainAsciiDocConfigs.set(buildCacheKey({ component, version }), asciidoc)
    })
  })
  const headerAsciiDocConfigs = new Map()
  const headerOverrides = { extensions: [], headerOnly: true }
  for (const [cacheKey, mainAsciiDocConfig] of mainAsciiDocConfigs) {
    headerAsciiDocConfigs.set(cacheKey, Object.assign({}, mainAsciiDocConfig, headerOverrides))
  }
  return contentCatalog
    .getPages((page) => page.out)
    .map((page) => {
      if (page.mediaType === 'text/asciidoc') {
        const asciidocConfig = headerAsciiDocConfigs.get(buildCacheKey(page.src))
        const { attributes } = (page.asciidoc = extractAsciiDocMetadata(
          loadAsciiDoc(page, contentCatalog, asciidocConfig || Object.assign({}, siteAsciiDocConfig, headerOverrides))
        ))
        Object.defineProperty(page, 'title', {
          get () {
            return this.asciidoc.doctitle
          },
        })
        registerPageAliases(attributes['page-aliases'], page, contentCatalog)
        if ('page-partial' in attributes) page.src.contents = page.contents
      }
      return page
    })
    .map((page) =>
      page.mediaType === 'text/asciidoc'
        ? convertDocument(page, contentCatalog, mainAsciiDocConfigs.get(buildCacheKey(page.src)) || siteAsciiDocConfig)
        : page
    )
    .map((page) => delete page.src.contents && page)
}

function buildCacheKey ({ component, version }) {
  return version + '@' + component
}

function registerPageAliases (aliases, targetFile, contentCatalog) {
  if (!aliases) return
  return aliases
    .split(',')
    .forEach((spec) => (spec = spec.trim()) && contentCatalog.registerPageAlias(spec, targetFile))
}

module.exports = Object.assign(convertDocuments, { convertDocuments, convertDocument })

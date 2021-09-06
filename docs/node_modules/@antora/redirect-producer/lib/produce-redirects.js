'use strict'

const computeRelativeUrlPath = require('@antora/asciidoc-loader/lib/util/compute-relative-url-path')
const File = require('vinyl')
const { URL } = require('url')

const ENCODED_SPACE_RX = /%20/g

/**
 * Produces redirects (HTTP redirections) for registered page aliases.
 *
 * Iterates over files in the alias family from the content catalog and creates artifacts that
 * handle redirects from the URL of each alias to the target URL. The artifact that is created
 * depends on the redirect facility in use. If the redirect facility is static (the default), this
 * function populates the contents of the alias file with an HTML redirect page (i.e., bounce page).
 * If the redirect facility is nginx, this function creates and returns an nginx configuration file
 * that contains rewrite rules for each alias. If the redirect facility is disabled, this function
 * unpublishes the alias files by removing the out property on each alias file.
 *
 * @memberof redirect-producer
 *
 * @param {Object} playbook - The configuration object for Antora.
 * @param {Object} playbook.site - Site-related configuration data.
 * @param {String} playbook.site.url - The base URL of the site.
 * @param {String} playbook.urls - URL-related configuration data.
 * @param {String} playbook.urls.redirectFacility - The redirect facility for
 *   which redirect configuration is being produced.
 * @param {ContentCatalog} contentCatalog - The content catalog that provides
 *   access to the virtual content files (i.e., pages) in the site.
 * @returns {Array<File>} An array of File objects that contain rewrite configuration for the web server.
 */
function produceRedirects (playbook, contentCatalog) {
  const aliases = contentCatalog.findBy({ family: 'alias' })
  if (!aliases.length) return []
  let siteUrl = playbook.site.url
  if (siteUrl) {
    if (siteUrl === '/') siteUrl = ''
    else if (siteUrl.charAt(siteUrl.length - 1) === '/') siteUrl = siteUrl.substr(0, siteUrl.length - 1)
  }
  switch (playbook.urls.redirectFacility) {
    case 'httpd':
      return createHttpdHtaccess(aliases, extractUrlPath(siteUrl))
    case 'netlify':
      return createNetlifyRedirects(
        aliases,
        extractUrlPath(siteUrl),
        (playbook.urls.htmlExtensionStyle || 'default') === 'default'
      )
    case 'nginx':
      return createNginxRewriteConf(aliases, extractUrlPath(siteUrl))
    case 'static':
      return populateStaticRedirectFiles(aliases, siteUrl)
    default:
      return unpublish(aliases)
  }
}

function extractUrlPath (url) {
  if (url) {
    if (url.charAt() === '/') return url
    const urlPath = new URL(url).pathname
    return urlPath === '/' ? '' : urlPath
  } else {
    return ''
  }
}

function createHttpdHtaccess (files, urlPath) {
  const rules = files.reduce((accum, file) => {
    delete file.out
    let fromUrl = file.pub.url
    fromUrl = ~fromUrl.indexOf('%20') ? `'${urlPath}${fromUrl.replace(ENCODED_SPACE_RX, ' ')}'` : urlPath + fromUrl
    let toUrl = file.rel.pub.url
    toUrl = ~toUrl.indexOf('%20') ? `'${urlPath}${toUrl.replace(ENCODED_SPACE_RX, ' ')}'` : urlPath + toUrl
    accum.push(`Redirect 301 ${fromUrl} ${toUrl}`)
    return accum
  }, [])
  return [new File({ contents: Buffer.from(rules.join('\n') + '\n'), out: { path: '.htaccess' } })]
}

// NOTE: a trailing slash on the pathname will be ignored
// see https://docs.netlify.com/routing/redirects/redirect-options/#trailing-slash
// however, we keep it when generating the rules for clarity
function createNetlifyRedirects (files, urlPath, includeDirectoryRedirects = false) {
  const rules = files.reduce((accum, file) => {
    delete file.out
    const fromUrl = urlPath + file.pub.url
    const toUrl = urlPath + file.rel.pub.url
    accum.push(`${fromUrl} ${toUrl} 301!`)
    if (includeDirectoryRedirects && fromUrl.endsWith('/index.html')) {
      accum.push(`${fromUrl.substr(0, fromUrl.length - 10)} ${toUrl} 301!`)
    }
    return accum
  }, [])
  return [new File({ contents: Buffer.from(rules.join('\n') + '\n'), out: { path: '_redirects' } })]
}

function createNginxRewriteConf (files, urlPath) {
  const rules = files.map((file) => {
    delete file.out
    let fromUrl = file.pub.url
    fromUrl = ~fromUrl.indexOf('%20') ? `'${urlPath}${fromUrl.replace(ENCODED_SPACE_RX, ' ')}'` : urlPath + fromUrl
    let toUrl = file.rel.pub.url
    toUrl = ~toUrl.indexOf('%20') ? `'${urlPath}${toUrl.replace(ENCODED_SPACE_RX, ' ')}'` : urlPath + toUrl
    return `location = ${fromUrl} { return 301 ${toUrl}; }`
  })
  return [new File({ contents: Buffer.from(rules.join('\n') + '\n'), out: { path: '.etc/nginx/rewrite.conf' } })]
}

function populateStaticRedirectFiles (files, siteUrl) {
  files.forEach((file) => (file.contents = Buffer.from(createStaticRedirectContents(file, siteUrl) + '\n')))
  return []
}

function createStaticRedirectContents (file, siteUrl) {
  const targetUrl = file.rel.pub.url
  const relativeUrl = computeRelativeUrlPath(file.pub.url, targetUrl)
  const canonicalUrl = siteUrl && siteUrl.charAt() !== '/' ? siteUrl + targetUrl : undefined
  const canonicalLink = canonicalUrl ? `<link rel="canonical" href="${canonicalUrl}">\n` : ''
  return `<!DOCTYPE html>
<meta charset="utf-8">
${canonicalLink}<script>location="${relativeUrl}"</script>
<meta http-equiv="refresh" content="0; url=${relativeUrl}">
<meta name="robots" content="noindex">
<title>Redirect Notice</title>
<h1>Redirect Notice</h1>
<p>The page you requested has been relocated to <a href="${relativeUrl}">${canonicalUrl || relativeUrl}</a>.</p>`
}

function unpublish (files) {
  files.forEach((file) => delete file.out)
  return []
}

module.exports = produceRedirects

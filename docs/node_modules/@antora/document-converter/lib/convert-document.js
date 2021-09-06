'use strict'

const { loadAsciiDoc, extractAsciiDocMetadata } = require('@antora/asciidoc-loader')

/**
 * Converts the contents on the specified file from AsciiDoc to embedded HTML.
 *
 * It first delegates to the AsciiDoc Loader to load the AsciiDoc contents on
 * the specified virtual file to a Document object. It then grabs the document
 * attributes from that Document and assigns them to the asciidoc.attributes
 * property on the file. If the document has a document header, it assigns the
 * doctitle, xreftext, and navtitle to the asciidoc property on that file. If
 * the page-partial attributes is set, it backs up the AsciiDoc source on the
 * src.contents property. It then converts the Document to embedded HTML, wraps
 * it in a Buffer, and assigns it to the contents property on the file. Finally,
 * the mediaType property is updated to 'text/html'.
 *
 * @memberof document-converter
 *
 * @param {File} file - The virtual file whose contents is an AsciiDoc source document.
 * @param {ContentCatalog} [contentCatalog=undefined] - The catalog of all virtual content files in the site.
 * @param {Object} [asciidocConfig={}] - AsciiDoc processor configuration options specific to this file.
 *
 * @returns {File} The virtual file that was converted.
 */
function convertDocument (file, contentCatalog = undefined, asciidocConfig = {}) {
  const doc = loadAsciiDoc(file, contentCatalog, asciidocConfig)
  if (!file.asciidoc) {
    file.asciidoc = extractAsciiDocMetadata(doc)
    if (doc.hasAttribute('page-partial')) file.src.contents = file.contents
  }
  file.contents = Buffer.from(doc.convert())
  file.mediaType = 'text/html'
  return file
}

module.exports = convertDocument

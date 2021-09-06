'use strict'

const Opal = global.Opal
const { $Antora } = require('../constants')
const $pageRefCallback = Symbol('pageRefCallback')
const $imageRefCallback = Symbol('imageRefCallback')

const Html5Converter = (() => {
  const scope = Opal.klass(
    Opal.module(null, 'Antora', $Antora),
    Opal.module(null, 'Asciidoctor').Converter.Html5Converter,
    'Html5Converter',
    function () {}
  )
  Opal.defn(scope, '$initialize', function initialize (backend, opts, callbacks) {
    Opal.send(this, Opal.find_super_dispatcher(this, 'initialize', initialize), [backend, opts])
    this[$pageRefCallback] = callbacks.onPageRef
    this[$imageRefCallback] = callbacks.onImageRef
  })
  Opal.defn(scope, '$inline_anchor', function convertInlineAnchor (node) {
    if (node.getType() === 'xref') {
      let callback
      let refSpec = node.getAttribute('path', undefined, false)
      if (refSpec && (callback = this[$pageRefCallback])) {
        const attrs = node.getAttributes()
        const fragment = attrs.fragment
        if (fragment && fragment !== Opal.nil) refSpec += '#' + fragment
        const { content, target, internal, unresolved } = callback(refSpec, node.getText())
        let type
        if (internal) {
          type = 'xref'
          delete attrs.path
          delete attrs.fragment
          attrs.refid = fragment // or target.substr(1)
        } else {
          type = 'link'
          attrs.role = `page${unresolved ? ' unresolved' : ''}${attrs.role ? ' ' + attrs.role : ''}`
        }
        const attributes = Opal.hash2(Object.keys(attrs), attrs)
        const options = Opal.hash2(['type', 'target', 'attributes'], { type, target, attributes })
        node = Opal.module(null, 'Asciidoctor').Inline.$new(node.getParent(), 'anchor', content, options)
      }
    }
    return Opal.send(this, Opal.find_super_dispatcher(this, 'inline_anchor', convertInlineAnchor), [node])
  })
  Opal.defn(scope, '$image', function convertImage (node) {
    return Opal.send(this, Opal.find_super_dispatcher(this, 'image', convertImage), [
      transformImageNode(this, node, node.getAttribute('target')),
    ])
  })
  Opal.defn(scope, '$inline_image', function convertInlineImage (node) {
    return Opal.send(this, Opal.find_super_dispatcher(this, 'inline_image', convertInlineImage), [
      transformImageNode(this, node, node.getTarget()),
    ])
  })
  return scope
})()

function transformImageNode (converter, node, imageTarget) {
  if (matchesResourceSpec(imageTarget)) {
    const imageRefCallback = converter[$imageRefCallback]
    if (imageRefCallback) {
      const alt = node.getAttribute('alt', undefined, false)
      if (node.isAttribute('default-alt', alt, false)) node.setAttribute('alt', alt.split(/[@:]/).pop())
      Opal.defs(node, '$image_uri', (imageSpec) => imageRefCallback(imageSpec) || imageSpec)
    }
  }
  if (node.hasAttribute('xref')) {
    const refSpec = node.getAttribute('xref', '', false)
    if (refSpec.charAt() === '#') {
      node.setAttribute('link', refSpec)
    } else if (~refSpec.indexOf('.adoc')) {
      const pageRefCallback = converter[$pageRefCallback]
      if (pageRefCallback) {
        const { target, unresolved } = pageRefCallback(refSpec, '[image]')
        const role = node.getAttribute('role', undefined, false)
        node.setAttribute('role', `link-page${unresolved ? ' link-unresolved' : ''}${role ? ' ' + role : ''}`)
        node.setAttribute('link', target)
      }
    } else {
      node.setAttribute('link', '#' + refSpec)
    }
  }
  return node
}

function matchesResourceSpec (target) {
  return ~target.indexOf(':')
    ? !(~target.indexOf('://') || (target.startsWith('data:') && ~target.indexOf(',')))
    : target.indexOf('@') > 0
}

module.exports = Html5Converter

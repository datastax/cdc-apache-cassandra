'use strict'

const convict = require('convict')
const json = require('json5')
const toml = require('@iarna/toml')
const yaml = require('js-yaml')
const { URL } = require('url')

const ARGS_SCANNER_RX = /(?:([^=,]+)|(?==))(?:,|$|=(|("|').*?\3|[^,]+)(?:,|$))/g
const PRIMITIVE_TYPES = [Boolean, Number, String]

/**
 * A convict function wrapper that registers custom formats and parsers and
 * isolates the configuration from the process environment by default.
 */
function solitaryConvict (schema, opts) {
  registerFormats(convict)
  registerParsers(convict)
  return convict(schema, opts || { args: [], env: {} })
}

function registerParsers (convict) {
  convict.addParser([
    { extension: 'json', parse: json.parse },
    { extension: 'toml', parse: toml.parse },
    { extension: 'yaml', parse: yaml.safeLoad },
    { extension: 'yml', parse: yaml.safeLoad },
    {
      extension: '*',
      parse: () => {
        throw new Error('Unexpected playbook file type (must be yml, json, or toml')
      },
    },
  ])
}

function registerFormats (convict) {
  convict.addFormat({
    name: 'map',
    validate: (val) => {
      if (!(val == null || val.constructor === Object)) throw new Error('must be a map (i.e., key/value pairs)')
    },
    coerce: (val, config, name) => {
      if (config == null) return val
      const accum = config.has(name) ? config.get(name) : {}
      let match
      ARGS_SCANNER_RX.lastIndex = 0
      while ((match = ARGS_SCANNER_RX.exec(val))) {
        const [, k, v] = match
        if (k) accum[k] = v ? (v === '-' ? '-' : yaml.safeLoad(v)) : ''
      }
      return accum
    },
  })
  convict.addFormat({
    name: 'primitive-map',
    validate: (val) => {
      if (val == null) return
      if (
        !(
          val.constructor === Object &&
          Object.entries(val).every(([k, v]) => k && (!v || ~PRIMITIVE_TYPES.indexOf(v.constructor)))
        )
      ) {
        throw new Error('must be a primitive map (i.e., key/value pairs, primitive values only)')
      }
    },
    coerce: (val, config, name) => {
      if (config == null) return val
      const accum = config.has(name) ? config.get(name) : {}
      let match
      ARGS_SCANNER_RX.lastIndex = 0
      while ((match = ARGS_SCANNER_RX.exec(val))) {
        const [, k, v] = match
        if (k) {
          let parsed
          if (v && v !== '-') {
            parsed = yaml.safeLoad(v)
            if (parsed && !~PRIMITIVE_TYPES.indexOf(parsed.constructor)) parsed = v
          } else {
            parsed = v || ''
          }
          accum[~k.indexOf('-') ? k.replace(/-/g, '_') : k] = parsed
        }
      }
      return accum
    },
  })
  convict.addFormat({
    name: 'boolean-or-string',
    validate: (val) => {
      if (!(val == null || val.constructor === String || val.constructor === Boolean)) {
        throw new Error('must be a boolean or string')
      }
    },
  })
  convict.addFormat({
    name: 'dir-or-virtual-files',
    validate: (val) => {
      if (!(val == null || val.constructor === String || Array.isArray(val))) {
        throw new Error('must be a directory path or list of virtual files')
      }
    },
  })
  convict.addFormat({
    name: 'url-or-pathname',
    validate: (val) => {
      if (val == null) return
      if (val.constructor === String) {
        if (val.charAt() === '/') val = 'https://example.org' + val
        let protocol
        let pathname
        try {
          ;({ protocol, pathname } = new URL(val))
        } catch (e) {
          throw new Error('must be an absolute URL or a pathname (i.e., root-relative path)')
        }
        if (protocol !== 'https:' && protocol !== 'http:') {
          throw new Error('must be an absolute URL or a pathname (i.e., root-relative path)')
        } else if (~pathname.indexOf('%20')) {
          throw new Error('must not contain spaces')
        }
      } else {
        throw new Error('must be an absolute URL or a pathname (i.e., root-relative path)')
      }
    },
  })
}

module.exports = solitaryConvict

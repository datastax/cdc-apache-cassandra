'use strict'

const camelCaseKeys = require('camelcase-keys')
const convict = require('./solitary-convict')
const fs = require('fs')
const { hasOwnProperty } = Object.prototype
const ospath = require('path')

/**
 * Builds a playbook object according to the provided schema from the specified
 * arguments and environment variables.
 *
 * Accepts an array of command line arguments (in the form of option flags and
 * switches) and a map of environment variables and translates this data into a
 * playbook object according the the specified schema. If no schema is
 * specified, the default schema provided by this package is used.
 *
 * @memberof playbook-builder
 *
 * @param {Array} [args=[]] - An array of arguments in the form of command line
 *   option flags and switches. Should begin with the first flag or switch.
 * @param {Object} [env={}] - A map of environment variables.
 * @param {Object} [schema=undefined] - A convict configuration schema.
 *
 * @returns {Object} A playbook object containing a hierarchical structure that
 *   mirrors the configuration schema. With the exception of the top-level asciidoc
 *   key and its descendants, all keys in the playbook are camelCased.
 */
function buildPlaybook (args = [], env = {}, schema = undefined) {
  const config = loadConvictConfig(args, env, schema)

  const relSpecFilePath = config.get('playbook')
  if (relSpecFilePath) {
    let absSpecFilePath = ospath.resolve(relSpecFilePath)
    if (ospath.extname(absSpecFilePath)) {
      if (!fs.existsSync(absSpecFilePath)) {
        let details = ''
        if (relSpecFilePath !== absSpecFilePath) {
          details = ` (path: ${relSpecFilePath}${ospath.isAbsolute(relSpecFilePath) ? '' : ', cwd: ' + process.cwd()})`
        }
        throw new Error(`playbook file not found at ${absSpecFilePath}${details}`)
      }
    } else if (fs.existsSync(absSpecFilePath + '.yml')) {
      absSpecFilePath += '.yml'
    } else if (fs.existsSync(absSpecFilePath + '.json')) {
      absSpecFilePath += '.json'
    } else if (fs.existsSync(absSpecFilePath + '.toml')) {
      absSpecFilePath += '.toml'
    } else {
      const details = `(path: ${relSpecFilePath}${ospath.isAbsolute(relSpecFilePath) ? '' : ', cwd: ' + process.cwd()})`
      throw new Error(
        `playbook file not found at ${absSpecFilePath}.yml, ${absSpecFilePath}.json, or ${absSpecFilePath}.toml ` +
          details
      )
    }
    config.loadFile(absSpecFilePath)
    if (relSpecFilePath !== absSpecFilePath) config.set('playbook', absSpecFilePath)
  }

  config.validate({ allowed: 'strict' })

  return exportModel(config)
}

function loadConvictConfig (args, env, customSchema) {
  return convict(customSchema || require('./config/schema'), { args, env })
}

function freeze (o) {
  let v
  for (const k in o) hasOwnProperty.call(o, k) && (Object.isFrozen((v = o[k])) || freeze(v))
  return Object.freeze(o)
}

function exportModel (config) {
  const schemaProperties = config._schema._cvtProperties
  const data = config.getProperties()
  if ('git' in schemaProperties && 'ensureGitSuffix' in schemaProperties.git._cvtProperties) {
    const git = data.git
    if (git.ensureGitSuffix != null) git.ensure_git_suffix = git.ensureGitSuffix
    delete git.ensureGitSuffix
  }
  if ('runtime' in schemaProperties && 'pull' in schemaProperties.runtime._cvtProperties) {
    const runtime = data.runtime
    if (runtime.pull != null) runtime.fetch = runtime.pull
    delete runtime.pull
  }
  if (
    'site' in schemaProperties &&
    'keys' in schemaProperties.site._cvtProperties &&
    '__private__google_analytics_key' in schemaProperties.site._cvtProperties
  ) {
    const site = data.site
    if (site.__private__google_analytics_key != null) site.keys.google_analytics = site.__private__google_analytics_key
    delete site.__private__google_analytics_key
  }
  const playbook = camelCaseKeys(data, { deep: true, stopPaths: ['asciidoc'] })
  playbook.dir = playbook.playbook ? ospath.dirname((playbook.file = playbook.playbook)) : process.cwd()
  delete playbook.playbook
  return freeze(playbook)
}

module.exports = buildPlaybook

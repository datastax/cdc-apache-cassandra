'use strict'

const { Command } = require('commander')
const { helpInformation } = Command.prototype
const { stringify } = JSON

// TODO include common options when outputting help for a (sub)command
Command.prototype.helpInformation = function () {
  const nonTTYColumns = 'columns' in process.stdout ? undefined : (process.stdout.columns = process.env.COLUMNS || 80)
  // NOTE override stringify to coerce to string normally
  JSON.stringify = (val) => `${val}`
  const helpInfo = helpInformation.call(this)
  JSON.stringify = stringify
  if (nonTTYColumns) delete process.stdout.columns
  return helpInfo
}

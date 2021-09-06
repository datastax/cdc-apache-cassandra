'use strict'

const { buildBaseUiModel, buildUiModel } = require('./build-ui-model')
const handlebars = require('handlebars')
const relativize = require('./helpers/relativize')
const resolvePage = require('./helpers/resolve-page')
const resolvePageURL = require('./helpers/resolve-page-url')
const requireFromString = require('require-from-string')

/**
 * Generates a function to wrap the page contents in a page layout.
 *
 * Compiles the Handlebars layouts, along with the partials and helpers, and
 * builds the shared site UI model. Passes these objects to a generated
 * function, which can then be used to apply a layout template to pages.
 *
 * @memberof page-composer
 *
 * @param {Object} playbook - The configuration object for Antora.
 * @param {ContentCatalog} contentCatalog - The content catalog
 *   that provides access to the virtual files in the site.
 * @param {UiCatalog} uiCatalog - The file catalog
 *   that provides access to the UI files for the site.
 * @param {Object} [env=process.env] - A map of environment variables.
 * @returns {Function} A function to compose a page (i.e., wrap the embeddable
 *   HTML contents in a standalone page layout).
 */
function createPageComposer (playbook, contentCatalog, uiCatalog, env = process.env) {
  handlebars.registerHelper('relativize', relativize)
  handlebars.registerHelper('resolvePage', resolvePage)
  handlebars.registerHelper('resolvePageURL', resolvePageURL)
  uiCatalog
    .findByType('helper')
    .forEach(({ path, stem, contents }) =>
      handlebars.registerHelper(stem, requireFromString(contents.toString(), path))
    )
  uiCatalog.findByType('partial').forEach(({ stem, contents }) => handlebars.registerPartial(stem, contents.toString()))
  const layouts = uiCatalog
    .findByType('layout')
    .reduce(
      (accum, { path: srcName, stem, contents }) =>
        accum.set(stem, handlebars.compile(contents.toString(), { preventIndent: true, srcName })),
      new Map()
    )
  return createPageComposerInternal(buildBaseUiModel(playbook, contentCatalog, env), layouts)
}

function createPageComposerInternal (baseUiModel, layouts) {
  /**
   * Wraps the embeddable HTML contents of the specified file in a page layout.
   *
   * Builds a UI model from the file and its context, executes on the specified
   * page layout on that model, and assigns the result to the contents property
   * of the file. If no layout is specified on the file, the default layout is
   * used.
   *
   * @memberof page-composer
   *
   * @param {File} file - The virtual file the contains embeddable HTML
   *   contents to wrap in a layout.
   * @param {ContentCatalog} _contentCatalog - The content catalog
   *   that provides access to the virtual files in the site (ignored).
   * @param {NavigationCatalog} navigationCatalog - The navigation catalog
   *   that provides access to the navigation for each component version.
   * @returns {File} The file whose contents were wrapped in the specified page layout.
   */
  return function composePage (file, _contentCatalog, navigationCatalog) {
    // QUESTION should we pass the playbook to the uiModel?
    const uiModel = buildUiModel(baseUiModel, file, baseUiModel.contentCatalog, navigationCatalog)
    let layout = uiModel.page.layout
    if (!layouts.has(layout)) {
      if (layout === '404') throw new Error('404 layout not found')
      const defaultLayout = uiModel.site.ui.defaultLayout
      if (defaultLayout === layout) {
        throw new Error(`${layout} layout not found`)
      } else if (!layouts.has(defaultLayout)) {
        throw new Error(`Neither ${layout} layout or fallback ${defaultLayout} layout found`)
      }
      // TODO log a warning that the default template is being used; perhaps on file?
      layout = defaultLayout
    }
    // QUESTION should we call trim() on result?
    try {
      file.contents = Buffer.from(layouts.get(layout)(uiModel))
    } catch (e) {
      throw transformHandlebarsError(e, layout)
    }
    return file
  }
}

function transformHandlebarsError ({ message, stack }, layout) {
  const m = stack.match(/^ *at Object\.ret \[as (.+?)\]/m)
  const templatePath = `${m ? 'partials/' + m[1] : 'layouts/' + layout}.hbs`
  const err = new Error(`${message}${~message.indexOf('\n') ? '\n^ ' : ' '}in UI template ${templatePath}`)
  err.stack = [err.toString()].concat(stack.substr(message.length + 8)).join('\n')
  return err
}

module.exports = createPageComposer

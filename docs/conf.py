# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "股票监控系统"
copyright = "2025, 开发团队"
author = "开发团队"
release = "1.0.0"
version = "1.0.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",  # 自动生成API文档
    "sphinx.ext.autosummary",  # 自动生成摘要
    "sphinx.ext.doctest",  # 文档测试
    "sphinx.ext.intersphinx",  # 交叉引用
    "sphinx.ext.todo",  # TODO扩展
    "sphinx.ext.coverage",  # 覆盖率报告
    "sphinx.ext.imgmath",  # 数学公式
    "sphinx.ext.mathjax",  # MathJax数学渲染
    "sphinx.ext.ifconfig",  # 条件配置
    "sphinx.ext.viewcode",  # 查看源码
    "sphinx.ext.githubpages",  # GitHub Pages支持
    "sphinx.ext.napoleon",  # Google/NumPy风格文档字符串
    "sphinx_rtd_theme",  # Read the Docs主题
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

language = "zh_CN"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

# 主题配置
html_theme_options = {
    "canonical_url": "",
    "analytics_id": "",
    "display_version": True,
    "prev_next_buttons_location": "bottom",
    "style_external_links": False,
    "vcs_pageview_mode": "",
    "style_nav_header_background": "#2980B9",
    "collapse_navigation": True,
    "sticky_navigation": True,
    "navigation_depth": 4,
    "includehidden": True,
    "titles_only": False,
}

# -- Options for autodoc extension -------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "exclude-members": "__weakref__",
}

# -- Options for autosummary extension ---------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autosummary.html#configuration

autosummary_generate = True

# -- Options for intersphinx extension ---------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html#configuration

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}

# -- Options for todo extension ----------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/todo.html#configuration

todo_include_todos = True

# -- Options for napoleon extension ------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html#configuration

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_use_keyword = True
napoleon_custom_sections = None

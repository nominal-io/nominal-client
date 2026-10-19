Core reference fragments.

.. core-navigation-start

.. raw:: html

   <script id="reference-pages" type="application/json">{{ core_reference_pages | tojson }}</script>

.. core-navigation-end

{% set data = module_data[fullname] %}
{% set all_classes = classes %}
{% set all_functions = functions %}
{% set all_exceptions = exceptions %}
{% for slug, topic in core_topics.items() %}
.. core-{{ slug }}-start

Modules: {% for module in topic.modules %}:py:mod:`{{ module }}`{{ ", " if not loop.last else "." }}{% endfor %}

{% for module in topic.modules %}
.. module:: {{ module }}
{% endfor %}

.. currentmodule:: {{ fullname }}

{% set classes = all_classes | select('in', topic.exports) | list %}
{% set functions = all_functions | select('in', topic.exports) | list %}
{% set exceptions = all_exceptions | select('in', topic.exports) | list %}
{% set attributes = topic.exports %}
{% set members = topic.exports %}
{% set data = dict(data, exports=topic.exports) %}
{% include "autosummary/reference-tables.rst" %}

.. core-{{ slug }}-end

{% endfor %}

.. currentmodule:: {{ fullname }}

.. autosummary::
   :toctree: ../generated
   :template: autosummary/orphan-object.rst
   :nosignatures:
{% for name in core_legacy_exports %}
   ~{{ name }}
{%- endfor %}

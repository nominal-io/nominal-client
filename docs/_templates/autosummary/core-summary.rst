Core reference fragments.

.. core-navigation-start

.. currentmodule:: {{ fullname }}

.. only:: html

   .. raw:: html

      <div hidden>

   .. autosummary::
      :nosignatures:
{% for name in module_data[fullname].exports %}
      ~{{ name }}
{%- endfor %}

   .. raw:: html

      </div>

.. core-navigation-end

{% set data = module_data[fullname] %}
{% set all_classes = classes %}
{% set all_functions = functions %}
{% set all_exceptions = exceptions %}
{% for slug, exports in core_topics.items() %}
.. core-{{ slug }}-start

Import from ``{{ fullname }}``.

.. currentmodule:: {{ fullname }}

{% set classes = all_classes | select('in', exports) | list %}
{% set functions = all_functions | select('in', exports) | list %}
{% set exceptions = all_exceptions | select('in', exports) | list %}
{% set attributes = exports %}
{% set members = exports %}
{% set data = dict(data, exports=exports) %}
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

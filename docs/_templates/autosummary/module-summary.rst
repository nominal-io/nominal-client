Module: ``{{ fullname }}``.

.. automodule:: {{ fullname }}
   :no-members:
   :no-inherited-members:
   :no-undoc-members:

{% set data = module_data[fullname] %}
{% for heading, names in [
    ('Classes', classes | reject('in', data.enums) | list),
    ('Enumerations', classes | select('in', data.enums) | list),
    ('Functions', functions),
    ('Exceptions', exceptions)
] if names %}
.. rubric:: {{ heading }}

.. autosummary::
   :toctree: ../generated
   :template: autosummary/object.rst
   :nosignatures:
{% for name in names | sort %}
   ~{{ name }}
{%- endfor %}

{% endfor %}

{% for heading, entries in [('Constants', data.constants), ('Type aliases', data.type_aliases)] %}
{% for name in members | sort if name in entries and name not in classes + functions + exceptions %}
{% if loop.first %}
.. rubric:: {{ heading }}

.. list-table::
   :class: autosummary
   :widths: 35 65

{% endif %}
   * - .. py:data:: {{ name }}
          :no-typesetting:

       :py:data:`~{{ fullname }}.{{ name }}`
     - {{ entries[name] }}
{% endfor %}
{% endfor %}

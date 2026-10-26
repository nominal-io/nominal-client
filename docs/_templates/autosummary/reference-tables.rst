{% set classes = classes | reject('in', data.type_aliases) | list %}
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

{% for heading, entries, names in [
    ('Constants', data.constants, attributes + data.exports),
    ('Type aliases', data.type_aliases, members)
] %}
{% for name in names | unique | sort if name in entries and name not in classes + functions + exceptions %}
{% if loop.first %}
.. rubric:: {{ heading }}

.. list-table::
   :class: autosummary
   :widths: 35 65

{% endif %}
   * - .. py:data:: {{ name }}
          :no-typesetting:{% if data.get('canonical_names', {}).get(name) %}
          :canonical: {{ data.canonical_names[name] }}{% endif %}

       :py:data:`~{{ fullname }}.{{ name }}`
     - {{ entries[name] }}
{% endfor %}
{% endfor %}

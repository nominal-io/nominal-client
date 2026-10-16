Module: ``{{ fullname }}``.

.. module:: {{ fullname }}

{% set data = namespace(names=[]) %}
{% for name in members | sort if not name.startswith('_') and name not in classes + functions + exceptions %}
{% set data.names = data.names + [name] %}
{% endfor %}
{% for heading, names in [('Classes', classes), ('Functions', functions), ('Exceptions', exceptions)] if names %}
.. rubric:: {{ heading }}

.. autosummary::
   :toctree: ../generated
   :template: autosummary/object.rst
   :nosignatures:
{% for name in names | sort %}
   ~{{ name }}
{%- endfor %}

{% endfor %}

{% if data.names %}
.. rubric:: Types and constants
{% for name in data.names %}
.. autodata:: {{ name }}

{% endfor %}
{% endif %}

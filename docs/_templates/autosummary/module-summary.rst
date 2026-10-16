Module: ``{{ fullname }}``.

.. automodule:: {{ fullname }}
   :no-members:
   :no-inherited-members:
   :no-undoc-members:

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

{% for name in members | sort if not name.startswith('_') and name not in classes + functions + exceptions %}
{% if loop.first %}
.. rubric:: Types and constants

{% endif %}
.. autodata:: {{ name }}

{% endfor %}

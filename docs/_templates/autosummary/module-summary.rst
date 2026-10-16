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

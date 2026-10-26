Module: ``{{ fullname }}``.

{% block module_description %}
.. automodule:: {{ fullname }}
   :no-members:
   :no-inherited-members:
   :no-undoc-members:

{% endblock %}

{% set data = module_data[fullname] %}
{% include "autosummary/reference-tables.rst" %}

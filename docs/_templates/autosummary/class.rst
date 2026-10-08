{{ objname | escape | underline }}

.. currentmodule:: {{ module }}

{#- Plain data types (enums, and classes without public methods) read best as one
    page with their fields inline; classes with methods get member tables with a
    page per member. Most nominal classes are dataclasses, so that alone doesn't
    tell the two apart. #}
{% set public_methods = (methods | reject("equalto", "__init__") | list) %}
{% if "__members__" in members or not public_methods %}
.. autoclass:: {{ objname }}
   :members:
{% else %}
.. autoclass:: {{ objname }}
   :no-members:
   :no-inherited-members:

{% if attributes %}
.. rubric:: Attributes

.. autosummary::
   :toctree:
   :nosignatures:
{% for item in attributes %}
   ~{{ name }}.{{ item }}
{%- endfor %}
{% endif %}

{% if public_methods %}
.. rubric:: Methods

.. autosummary::
   :toctree:
   :nosignatures:
{% for item in public_methods %}
   ~{{ name }}.{{ item }}
{%- endfor %}
{% endif %}
{% endif %}

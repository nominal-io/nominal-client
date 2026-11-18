{{ objname | escape | underline }}

.. currentmodule:: {{ module }}

{% if fullname in class_aliases | default({}) %}
.. py:class:: {{ objname }}

   Compatibility import for :py:class:`{{ class_aliases[fullname] }}`.
{% else %}
.. auto{{ objtype }}:: {{ objname }}
{% endif %}

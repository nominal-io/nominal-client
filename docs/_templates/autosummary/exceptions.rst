{{ fullname | escape | underline }}

Exceptions
----------

Errors raised by the client, from ``{{ fullname }}``. All errors derive from
:exc:`~nominal.exceptions.NominalError`.

.. autosummary::
   :toctree: generated
   :template: autosummary/object.rst
   :nosignatures:
{% for name in exceptions if name not in warning_names %}
   ~{{ fullname }}.{{ name }}
{%- endfor %}

Warnings
--------

Warnings emitted by the client, from ``{{ fullname }}``.

.. autosummary::
   :toctree: generated
   :template: autosummary/object.rst
   :nosignatures:
{% for name in exceptions if name in warning_names %}
   ~{{ fullname }}.{{ name }}
{%- endfor %}

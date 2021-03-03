
{{ name }}
{{ underline }}

.. autoclass:: {{ fullname }}
   :members:

   {% block methods %}
   .. automethod:: __init__

   {% if methods %}
   .. rubric:: Methods
   .. autosummary::
   {% for item in methods %}
      ~{{ name }}.{{ item }}
   {%- endfor %}
   
    
   {% for item in attributes %}
      ~{{ name }}.{{ item }}
   {%- endfor %}

   {% endif %}
   {% endblock %}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: {{ _('Attributes') }}

   .. autosummary::
   {% for item in attributes %}
      ~{{ name }}.{{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}
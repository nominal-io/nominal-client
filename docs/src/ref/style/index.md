# style

Nominal's look for plots. One palette, applied to a plotting library with `use`. For plotly, the style is wide and
transparent, with faint gridlines and grey text, so figures sit on the page in both dark and light themes.
Install plotly with `nominal[plotly]`.

```python
import nominal.style

nominal.style.use("plotly")   # every plotly figure made afterwards uses it
df.nom.wide().plot(backend="plotly")
```

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.style.rst
```

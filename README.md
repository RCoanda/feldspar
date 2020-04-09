<span align="center">
  <pre>
    <a href="https://github.com/xcavation/feldspar">
      <img src="https://raw.githubusercontent.com/xcavation/feldspar/develop/readme_cover.jpg" align="center">
    </a>
    Photo by Stephen Leonardi on Unsplash
  </pre>
</span>

# feldspar 🧱
Foundational process mining library.

```python
>>> from feldspar import TraceGenerator

>>> filepath = "https://raw.githubusercontent.com/xcavation/feldspar/feature/base-setup/data/running-example.xes"

>>> L = TraceGenerator.from_file(filepath)

>>> L = L.filter(lambda trace: len(trace) < 5)
>>> L = L.map(lambda trace: tuple(event["concept:name"] for event in trace))
>>> L = L.shuffle()

>>> for trace in L:
...   # do stuff with the trace
```



# feldspar 🧱
Foundational process mining library.

```python
>>> from feldspar import TraceFeeder

>>> filepath = "https://raw.githubusercontent.com/xcavation/feldspar/feature/base-setup/data/running-example.xes"
>>> L = TraceFeeder.from_file(filepath)

>>> L = L.map(lambda trace: tuple(event["concept:name"] for event in trace))
>>> L = L.filter(lambda trace: len(trace) < 5)
>>> L = L.shuffle()

>>> for trace in L:
...   print(trace)
("register request", "examine thoroughly", "check ticket", "decide", "reject request")
("register request", "check ticket", "examine casually", "decide", "pay compensation")
("register request", "check ticket", "examine thoroughly", "decide", "reject request")
("register request", "examine casually", "check ticket", "decide", "pay compensation")
```

<span align="center">
  <pre>
    <a href="https://github.com/xcavation/feldspar">
      <img str="https://raw.githubusercontent.com/xcavation/feldspar/develop/readme_cover.jpg" align="center">
    </a>
    Photo by Stephen Leonardi on Unsplash
  </pre>
</span>

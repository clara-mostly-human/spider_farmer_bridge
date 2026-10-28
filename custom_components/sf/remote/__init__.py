"""Run the proxy in a separate process (the Spider Farmer Bridge app).

``protocol`` is shared by both sides and imports nothing from Home Assistant;
``client`` is the Home Assistant side. The app side lives in ``bridge/`` at the
repository root.
"""

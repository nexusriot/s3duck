"""Back and forward across visited locations."""


class HistoryMixin:
    """Back and forward across visited locations."""

    @classmethod
    def push_history(cls, entry, location):
        """
        Record a move in a tab's trail.

        A move made after going back truncates the forward branch, the way a
        browser does — otherwise "forward" would offer a route the user has
        already left.
        """
        history = entry.setdefault("history", [])
        position = int(entry.get("pos", len(history) - 1))
        if history and 0 <= position < len(history) \
                and history[position] == location:
            return
        del history[position + 1:]
        history.append(location)
        if len(history) > cls.HISTORY_DEPTH:
            del history[:len(history) - cls.HISTORY_DEPTH]
        entry["pos"] = len(history) - 1

    def can_go_back(self) -> bool:
        entry = self.current_tab_entry()
        return bool(entry) and int(entry.get("pos", 0)) > 0

    def can_go_forward(self) -> bool:
        entry = self.current_tab_entry()
        if not entry:
            return False
        return int(entry.get("pos", 0)) < len(entry.get("history", [])) - 1

    def _step_history(self, delta):
        entry = self.current_tab_entry()
        if not entry:
            return
        history = entry.get("history") or []
        old_position = int(entry.get("pos", 0))
        position = old_position + int(delta)
        if not (0 <= position < len(history)):
            return
        # Asked before anything moves: a refused step used to leave the
        # position moved and the flag below set, so the NEXT real move went
        # unrecorded and Back from a/b/c/ landed at the bucket root.
        if self._refuse_navigation():
            return
        entry["pos"] = position
        bucket, prefix = history[position]
        # The move itself must not be recorded, or back would bounce between
        # two entries forever. It carries where it came from, so a step whose
        # listing then fails can be taken back.
        self._history_navigating = (entry, old_position)
        self.open_location(bucket, prefix)
        self._update_history_buttons()

    def _abandon_history_step(self):
        """A Back/Forward whose location never loaded: undo its position."""
        step = self._history_navigating
        self._history_navigating = False
        if isinstance(step, tuple):
            entry, old_position = step
            entry["pos"] = old_position
        self._update_history_buttons()

    def goBack(self):
        """Back to the previous location in this tab's trail (Alt+Left)."""
        self._step_history(-1)

    def goForward(self):
        """Forward again after a Back (Alt+Right)."""
        self._step_history(1)

    def _update_history_buttons(self):
        for name, enabled in (("btnBack", self.can_go_back()),
                              ("btnForward", self.can_go_forward())):
            action = getattr(self, name, None)
            if action is not None:
                action.setEnabled(enabled)

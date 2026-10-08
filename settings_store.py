"""One typed front door to QSettings.

Reading a single value used to take three statements and name its group by
hand::

    self.settings.beginGroup("common")
    raw = self.settings.value("listing_limit", DEFAULT_LISTING_LIMIT)
    self.settings.endGroup()

That ran 31 times in the window alone, and the coercion that has to follow --
QSettings hands back strings, so an int setting needs its own try/except --
was written out again at each site, or forgotten. Six per-profile key names
were each built from the same f-string, and one of the six spelled the
fallback differently, so a profile with no name read a different key than its
five neighbours did.

The group is a constructor argument, the coercions are methods, and
``profile_key`` is written once.
"""

from contextlib import contextmanager

DEFAULT_GROUP = "common"


class SettingsStore:
    """A QSettings scoped to one group, with the coercions spelled once."""

    def __init__(self, qsettings, profile_name="", group=DEFAULT_GROUP):
        """``profile_name`` may be a string or a callable returning one.

        Switching profiles renames the window's profile in place, and the
        per-profile keys have to follow it, so a store built once at startup
        cannot snapshot the name.
        """
        self._settings = qsettings
        self._group = group
        self._profile_name = profile_name

    @property
    def profile_name(self) -> str:
        name = self._profile_name
        return str((name() if callable(name) else name) or "")

    @property
    def qsettings(self):
        """The underlying QSettings, for the few callers that need it raw."""
        return self._settings

    def scoped(self, group):
        """The same settings seen through a different group."""
        return SettingsStore(self._settings, self._profile_name, group)

    @contextmanager
    def group(self, name=None):
        """Hold the group open across several reads or writes."""
        self._settings.beginGroup(name or self._group)
        try:
            yield self._settings
        finally:
            self._settings.endGroup()

    def profile_key(self, name) -> str:
        """``"<name>/<profile>"`` -- the per-profile form, written once.

        The ``or "default"`` matters: a profile with an empty name has to land
        somewhere, and it has to be the same somewhere every time.
        """
        return f"{name}/{self.profile_name or 'default'}"

    def get(self, key, default=None):
        with self.group() as settings:
            value = settings.value(key, default)
        return default if value is None else value

    def get_str(self, key, default="") -> str:
        value = self.get(key, default)
        return "" if value is None else str(value)

    def get_int(self, key, default=0) -> int:
        """The stored int, or ``default`` when it is missing or unreadable.

        QSettings round-trips through strings on some backends, so a value
        written as an int can come back as "4" -- or, after a hand edit, as
        "four".
        """
        try:
            return int(self.get(key, default))
        except (TypeError, ValueError):
            return int(default)

    def get_bool(self, key, default=False) -> bool:
        """Qt writes booleans as "true"/"false"; Python writes "True"/"False"."""
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("true", "1", "yes", "on")

    def set(self, key, value):
        with self.group() as settings:
            settings.setValue(key, value)

    def set_many(self, values):
        """Several keys under one open group."""
        with self.group() as settings:
            for key, value in dict(values).items():
                settings.setValue(key, value)

    def remove(self, key):
        with self.group() as settings:
            settings.remove(key)

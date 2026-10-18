"""MainWindow, in the pieces it is actually made of.

The window was one class of 240 methods and 137 attributes. The methods
cluster cleanly -- tabs, the transfer queue, navigation, the watch, saved
places, credentials -- and each cluster is a mixin here.

A mixin rather than a collaborator object, for now, because every method
keeps the same ``self``: the move changes no call site and the suite checks
the move rather than a rewrite. Splitting the state out into objects that own
it is the next step, taken per cluster where it pays for itself.
"""

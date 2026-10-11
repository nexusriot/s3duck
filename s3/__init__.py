"""The boto3 layer, in the pieces Model is actually made of.

Model stays the single front door -- everything in the app holds one and
calls it -- but its 130 methods cluster by what they do to a bucket, and each
cluster is a mixin here. Same `self`, so no call site changed; what it buys
is that upload resume logic is no longer 4,000 lines away from download
verification.
"""

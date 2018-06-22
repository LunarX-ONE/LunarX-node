# LunarX-node

>https://LunarX.co

LunarX node is the engine part of the whole network, running on individual devices to provide data chain services

>src-x-node

The entrance of the node.

>src-x-virtual-fs

The local vertual file system to manage blocks. Major functionaliy includes garbage collecting, blocks generating, blocks merging.

>src-x-io

The IO module for upper layer services. This module, for the concern of performance, we use C++ to develop, and wrapped by JNI for invoking on JAVA side.
 
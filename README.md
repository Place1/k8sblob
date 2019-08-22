# k8sblob

**This is currently an experiment**

A gocloud blob implementation for storing objects in kubernetes via config maps

You can use it with your current go cloud project by importing the driver

```
import (
  _ "github.com/place1/k8sblob"
)
```

_but why_

I thought it'd be really cool to extend [pulumi](https://www.pulumi.com/) to be able
to store it's statefiles in kubernetes natively via ConfigMap objects.

The motivation is to use pulumi with on-prem kubernetes clusters that don't have access
to cloud object storage or the pulumi SaaS. Other use cases are to use pulumi with
minikube locally.

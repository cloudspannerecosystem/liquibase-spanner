## Deploying a Version to Maven Central

### Prerequisites

1. A Sonatype Jira account: https://issues.sonatype.org/secure/Signup!default.jspa
2. Permission for your Jira account to publish to the group id 'com.google.cloudspannerecosystem'. This can be requested through Jira.
3. A settings.xml file for Maven (in ~/.m2/settings.xml) containing at least the following information:

```xml
<settings>
  <servers>
    <server>
      <id>ossrh</id>
      <username>your-jira-id</username>
      <password>your-jira-pwd</password>
    </server>
  </servers>
</settings>
```

### Deploying to Maven Central

1. Make sure everything builds and the correct version number has been set in the pom.xml
2. Execute `mvn clean deploy -Prelease`

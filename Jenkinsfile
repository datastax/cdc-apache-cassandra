#!groovy

def initializeEnvironment() {
  env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
  env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  env.GITHUB_BRANCH_URL = "${GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
  env.GITHUB_COMMIT_URL = "${GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"
  env.BLUE_OCEAN_URL = "${JENKINS_URL}/blue/organizations/jenkins/tools%2Fdsbulk/detail/${BRANCH_NAME}/${BUILD_NUMBER}"

  env.MAVEN_HOME = "${env.HOME}/.mvn/apache-maven-3.2.5"
  env.PATH = "${env.MAVEN_HOME}/bin:${env.PATH}"

  env.JAVA_HOME = sh(label: 'Get JAVA_HOME', script: '''#!/bin/bash -le
    . ${JABBA_SHELL}
    jabba which ${JABBA_VERSION}''', returnStdout: true).trim()

  sh label: 'Display Java and environment information', script: '''#!/bin/bash -le
    . ${JABBA_SHELL}
    jabba use ${JABBA_VERSION}

    java -version

    printenv | sort
  '''
}

def recordTestResults() {
  junit testResults: '**/target/surefire-reports/TEST-*.xml', allowEmptyResults: false
  junit testResults: '**/target/failsafe-reports/TEST-*.xml', allowEmptyResults: false
}

def recordCodeCoverage() {
  /*
  if (env.CASSANDRA_VERSION.startsWith("3.11")) {
    jacoco(
            execPattern: '**/target/**.exec',
            exclusionPattern: '**/generated/**'
    )
  }
  */
}

def recordArtifacts() {
  /*
  if (params.GENERATE_DISTRO && env.CASSANDRA_VERSION.startsWith("3.11")) {
    archiveArtifacts artifacts: 'distribution/target/dsbulk-*.tar.gz', fingerprint: true
  }
  */
}

def notifySlack(status = 'started') {

  if (!params.SLACK_ENABLED) {
    return
  }

  if (status == 'started' || status == 'completed') {
    // started and completed events are now disabled
    return
  }

  if (status == 'started') {
    if (env.SLACK_START_NOTIFIED == 'true') {
      return
    }
    // Set the global pipeline scoped environment (this is above each matrix)
    env.SLACK_START_NOTIFIED = 'true'
  }

  def event = status
  if (status == 'started') {
    String causes = "${currentBuild.buildCauses}"
    def startedByUser = causes.contains('User')
    def startedByCommit = causes.contains('Branch')
    def startedByTimer = causes.contains('Timer')
    if (startedByUser) {
      event = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')[0].shortDescription.toLowerCase()
    } else if (startedByCommit) {
      event = "was triggered on commit"
    } else if (startedByTimer) {
      event = "was triggered by timer"
    }
  } else {
    event = "${status == 'failed' ? status.toUpperCase() : status} after ${currentBuild.durationString - ' and counting'}"
  }

  String buildUrl = env.BLUE_OCEAN_URL == null ?
          "#${env.BUILD_NUMBER}" :
          "<${env.BLUE_OCEAN_URL}|#${env.BUILD_NUMBER}>"

  String branchUrl = env.GITHUB_BRANCH_URL == null ?
          "${env.BRANCH_NAME}" :
          "<${env.GITHUB_BRANCH_URL}|${env.BRANCH_NAME}>"

  String commitUrl = env.GIT_SHA == null ?
          "commit unknown" :
          env.GITHUB_COMMIT_URL == null ?
                  "${env.GIT_SHA}" :
                  "<${env.GITHUB_COMMIT_URL}|${env.GIT_SHA}>"

  String message = "Build ${buildUrl} on branch ${branchUrl} (${commitUrl}) ${event}."

  def color = 'good' // Green
  if (status == 'aborted') {
    color = '808080' // Grey
  } else if (status == 'unstable') {
    color = 'warning' // Orange
  } else if (status == 'failed') {
    color = 'danger' // Red
  }

  slackSend channel: "#cassandra-source-connector",
            message: "${message}",
            color: "${color}"
}

// branch pattern for cron
// should match 3.x, 4.x, 4.5.x, etc
def branchPatternCron = ~"\\d+(\\.\\d+)*\\.x"

pipeline {
  agent none

  options {
    timeout(time: 4, unit: 'HOURS')
    buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
                              numToKeepStr: '50'))        // Keep only the last 50 build records
  }

  parameters {
    booleanParam(
      name: 'SLACK_ENABLED',
      defaultValue: false,
      description: 'Flag to determine if Slack notifications should be sent')
  }

  environment {
    OS_VERSION = 'ubuntu/bionic64/java-driver'
    JABBA_SHELL = '/usr/lib/jabba/jabba.sh'
    JABBA_VERSION = '1.8'
    // always run all tests when generating the distribution tarball
    ENABLE_MEDIUM_PROFILE = "${params.RUN_LONG_TESTS || params.RUN_VERY_LONG_TESTS || params.GENERATE_DISTRO}"
    ENABLE_LONG_PROFILE = "${params.RUN_VERY_LONG_TESTS || params.GENERATE_DISTRO}"
    ENABLE_RELEASE_PROFILE = "${params.GENERATE_DISTRO}"
  }

  stages {
    stage('default') {
      agent {
        label "${OS_VERSION}"
      }
      environment {
        JABBA_VERSION = '1.8'
      }
      stages {
        stage('Initialize-Environment') {
          steps {
            initializeEnvironment()
          }
        }
        stage('Test') {
          steps {
            script {
              try {
                sh './gradlew clean build -x agent-c3-luna -x agent-c3-pulsar -x agent-c4-luna -x agent-c4-pulsar --no-daemon' //run a gradle task
              } finally {
                junit '**/build/test-results/test/*.xml'
                //make the junit test results available in any case (success & failure)
              }
            }
          }
        }
        stage('Assemble') {
          steps {
            sh './gradlew assemble'
          }
        }
      }
    }
  }
}

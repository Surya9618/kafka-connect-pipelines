// vars/drSwitchPipeline.groovy
//
// Shared pipeline logic for both DR switch directions.
// Called from Jenkinsfile.switchover and Jenkinsfile.switchback with a
// fixed 'direction' so the operator's only choice is which JOB to run,
// not a parameter that could be picked wrong.
//
// Usage (from a Jenkinsfile):
//   @Library('kafka-dr-lib') _
//   drSwitchPipeline(direction: 'primary-to-dr', approvers: 'dr-approvers')

def call(Map config) {
    if (!config.direction) {
        error("drSwitchPipeline: 'direction' is required (primary-to-dr or dr-to-primary)")
    }

    pipeline {
        agent any

        options {
            timestamps()
            disableConcurrentBuilds()
        }

        parameters {
            string(name: 'CONNECTOR_LIST', defaultValue: 'ALL',
                   description: 'Comma-separated connector names, or ALL')
            booleanParam(name: 'DRY_RUN', defaultValue: true,
                   description: 'If true: only checkout/validate/backup/diff. No approval, no apply, no verify, no audit push.')
            booleanParam(name: 'PUSH_AUDIT', defaultValue: false,
                   description: 'Push the audit commit to the git remote after apply (requires git-push-creds).')
        }

        stages {

            stage('Checkout') {
                steps {
                    checkout scm
                }
            }

            stage('Validate mapping') {
                steps {
                    sh """
                        python3 scripts/apply_config.py \
                            --direction ${config.direction} \
                            --validate-only \
                            --connectors '${params.CONNECTOR_LIST}'
                    """
                }
            }

            stage('Backup current configs') {
                steps {
                    sh """
                        python3 scripts/apply_config.py \
                            --direction ${config.direction} \
                            --backup-only \
                            --connectors '${params.CONNECTOR_LIST}'
                    """
                }
            }

            stage('Show diff') {
                steps {
                    sh """
                        python3 scripts/apply_config.py \
                            --direction ${config.direction} \
                            --diff-only \
                            --connectors '${params.CONNECTOR_LIST}'
                    """
                }
            }

            stage('Approval') {
                when { expression { !params.DRY_RUN } }
                steps {
                    input message: "Confirm ${config.direction} DR switch for connectors: ${params.CONNECTOR_LIST}?",
                          submitter: config.approvers ?: null
                }
            }

            stage('Apply') {
                when { expression { !params.DRY_RUN } }
                steps {
                    sh """
                        python3 scripts/apply_config.py \
                            --direction ${config.direction} \
                            --apply \
                            --connectors '${params.CONNECTOR_LIST}'
                    """
                }
            }

            stage('Verify') {
                when { expression { !params.DRY_RUN } }
                steps {
                    sh """
                        python3 scripts/apply_config.py \
                            --direction ${config.direction} \
                            --verify \
                            --connectors '${params.CONNECTOR_LIST}'
                    """
                }
            }

            stage('Commit audit trail') {
                when { expression { !params.DRY_RUN } }
                steps {
                    script {
                        if (params.PUSH_AUDIT) {
                            withCredentials([usernamePassword(
                                credentialsId: 'git-push-creds',
                                usernameVariable: 'GIT_USER',
                                passwordVariable: 'GIT_TOKEN'
                            )]) {
                                sh """
                                    git config user.email 'jenkins-dr-bot@yourorg.com'
                                    git config user.name 'jenkins-dr-bot'
                                    python3 scripts/apply_config.py --direction ${config.direction} --commit-audit
                                """
                                sh """
                                    REMOTE_HOST_PATH=\$(git config --get remote.origin.url | sed -E "s#https?://##")
                                    git push "https://\${GIT_USER}:\${GIT_TOKEN}@\${REMOTE_HOST_PATH}" HEAD:refs/heads/${config.targetBranch ?: 'main'}
                                """
                            }
                        } else {
                            sh "python3 scripts/apply_config.py --direction ${config.direction} --commit-audit"
                            echo 'Audit commit created in the Jenkins workspace but NOT pushed (PUSH_AUDIT=false). Push manually if needed.'
                        }
                    }
                }
            }
        }

        post {
            failure {
                echo "Pipeline failed for direction=${config.direction}. Pre-change connector configs (if backup stage completed) are under backups/ in this build's archived artifacts — use them to manually restore if needed."
            }
            always {
                archiveArtifacts artifacts: 'backups/**', allowEmptyArchive: true
            }
        }
    }
}

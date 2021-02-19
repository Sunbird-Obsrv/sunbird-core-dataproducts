node('build-slave') {
    try {
        String ANSI_GREEN = "\u001B[32m"
        String ANSI_NORMAL = "\u001B[0m"
        String ANSI_BOLD = "\u001B[1m"
        String ANSI_RED = "\u001B[31m"
        String ANSI_YELLOW = "\u001B[33m"
        ansiColor('xterm') {
            stage('Checkout') {
                cleanWs()
                checkout scm
                commit_hash = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                artifact_version = sh(script: "echo " + params.github_release_tag.split('/')[-1] + "_" + commit_hash + "_" + env.BUILD_NUMBER, returnStdout: true).trim()
                echo "artifact_version: "+ artifact_version
            }
        }
        stage('Pre-Build') {
            sh '''
                #sed -i "s/'replication_factor': '2'/'replication_factor': '1'/g" scripts/database/data.cql
                '''
        }
        stage('Build') {
            sh '''
                mvn clean install -DskipTests
                '''
        }
        stage('Archive artifacts'){
            sh """
                        mkdir lpa_core_dp_artifacts
                        cp batch-models/target/batch-models-2.0.jar lpa_core_dp_artifacts
                        cp job-manager/target/job-manager-2.0.jar lpa_core_dp_artifacts
                        zip -j lpa_core_dp_artifacts.zip:${artifact_version} lpa_core_dp_artifacts/*
                    """
            archiveArtifacts artifacts: "lpa_core_dp_artifacts.zip:${artifact_version}", fingerprint: true, onlyIfSuccessful: true
            sh """echo {\\"artifact_name\\" : \\"lpa_core_dp_artifacts.zip\\", \\"artifact_version\\" : \\"${artifact_version}\\", \\"node_name\\" : \\"${env.NODE_NAME}\\"} > metadata.json"""
            archiveArtifacts artifacts: 'metadata.json', onlyIfSuccessful: true
            currentBuild.description = artifact_version
        }
    }
    catch (err) {
        currentBuild.result = "FAILURE"
        throw err
    }
}

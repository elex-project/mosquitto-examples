plugins {
    java
}

group = "kr.pe.elex"
version = "1.0-SNAPSHOT"
java {
    sourceCompatibility = org.gradle.api.JavaVersion.VERSION_11
    targetCompatibility = org.gradle.api.JavaVersion.VERSION_11
}

configurations {
    compileOnly {
        extendsFrom(annotationProcessor.get())
    }
    testCompileOnly {
        extendsFrom(testAnnotationProcessor.get())
    }
}
tasks.compileJava {
    options.encoding = "UTF-8"
}

tasks.compileTestJava {
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnitPlatform()
}
repositories {
    repositories {
        maven {
            url = uri("https://repository.elex-project.com/repository/maven")
        }
    }
}

dependencies {
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("ch.qos.logback:logback-classic:1.2.3")

    compileOnly("org.projectlombok:lombok:1.18.16")
    annotationProcessor("org.projectlombok:lombok:1.18.16")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.16")

    //implementation("org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5")
    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.0")
}

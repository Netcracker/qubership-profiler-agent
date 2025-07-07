import java.time.Duration

plugins {
    id("build-logic.build-params")
    id("com.gradleup.nmcp.aggregation")
}

nmcpAggregation {
    centralPortal {
        username = providers.environmentVariable("CENTRAL_PORTAL_USERNAME")
        password = providers.environmentVariable("CENTRAL_PORTAL_PASSWORD")
        publishingType = buildParameters.centralPortalPublishingType.name
        validationTimeout = Duration.ofMinutes(buildParameters.centralPortalValidationTimeout.toLong())
    }
}

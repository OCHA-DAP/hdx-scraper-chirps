<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis hasScaleBasedVisibilityFlag="0" minScale="1e+08" styleCategories="AllStyleCategories" version="3.16.12-Hannover" maxScale="0">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
  </flags>
  <temporal fetchMode="0" enabled="0" mode="0">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <customproperties>
    <property key="WMSBackgroundLayer" value="false"/>
    <property key="WMSPublishDataSourceUrl" value="false"/>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="identify/format" value="Value"/>
  </customproperties>
  <pipe>
    <provider>
      <resampling zoomedOutResamplingMethod="nearestNeighbour" enabled="false" zoomedInResamplingMethod="nearestNeighbour" maxOversampling="2"/>
    </provider>
    <rasterrenderer nodataColor="" band="1" opacity="1" classificationMax="750" alphaBand="-1" classificationMin="-750" type="singlebandpseudocolor">
      <rasterTransparency/>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <rastershader>
        <colorrampshader labelPrecision="1" minimumValue="-750" classificationMode="1" colorRampType="DISCRETE" clip="0" maximumValue="750">
          <colorramp name="[source]" type="gradient">
            <prop v="215,25,28,255" k="color1"/>
            <prop v="44,123,182,255" k="color2"/>
            <prop v="0" k="discrete"/>
            <prop v="gradient" k="rampType"/>
            <prop v="0.25;253,174,97,255:0.5;255,255,191,255:0.75;171,217,233,255" k="stops"/>
          </colorramp>
          <item color="#69191a" label="&lt;= -300" value="-300" alpha="255"/>
          <item color="#a31e1e" label="-300 - -200" value="-200" alpha="255"/>
          <item color="#ff3300" label="-200 - -100" value="-100" alpha="255"/>
          <item color="#ffa200" label="-100 - -50" value="-50" alpha="255"/>
          <item color="#ffe978" label="-50 - -25" value="-25" alpha="255"/>
          <item color="#f0dcb9" label="-25 - -10" value="-10" alpha="255"/>
          <item color="#fefefe" label="-10 - 10" value="10" alpha="255"/>
          <item color="#77eb73" label="10 - 25" value="25" alpha="255"/>
          <item color="#b5ebfa" label="25 - 50" value="50" alpha="255"/>
          <item color="#78c6fa" label="50 - 100" value="100" alpha="255"/>
          <item color="#3a95f5" label="100 - 200" value="200" alpha="255"/>
          <item color="#1e6deb" label="200 - 300" value="300" alpha="255"/>
          <item color="#254061" label="> 300" value="inf" alpha="255"/>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast gamma="1" brightness="0" contrast="0"/>
    <huesaturation saturation="0" colorizeBlue="128" colorizeStrength="100" colorizeOn="0" grayscaleMode="0" colorizeRed="255" colorizeGreen="128"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <blendMode>0</blendMode>
</qgis>

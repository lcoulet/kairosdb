package org.kairosdb.publisher.skyline;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.kairosdb.core.DataPointListener;

/**
 *
 * @author lcoulet
 */
public class PublisherModule extends AbstractModule
{
	@Override
	protected void configure()
	{
		bind(SkylineDataPublisher.class).in(Scopes.SINGLETON);
	}
}

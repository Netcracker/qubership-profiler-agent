import AppHeader from '@app/components/app-header/app-header';
import { render, screen } from '@testing-library/react';

test.skip('should have a app title in header', () => {
    render(<AppHeader />);
    expect(screen.getByText('App Name')).toBeInTheDocument();
});

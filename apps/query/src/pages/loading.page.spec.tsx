import LoadingPage from '@app/pages/loading.page';
import { render, screen } from '@testing-library/react';

test('should display text when it passed', () => {
    render(<LoadingPage text="text of loading" />);

    expect(screen.getByText('text of loading')).toBeInTheDocument();
});

test('should change text on rerender', () => {
    render(<LoadingPage text="text of loading" />);

    expect(screen.getByText('text of loading')).toBeInTheDocument();

    render(<LoadingPage text="changed text" />);

    expect(screen.getByText('changed text')).toBeInTheDocument();
});

test('should match snapshot', () => {
    const { container } = render(<LoadingPage text="text of loading" />);

    expect(container).toMatchSnapshot();
});
